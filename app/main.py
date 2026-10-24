import asyncio
import hashlib
import hmac
import secrets
import re
import time
import httpx
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Literal
from uuid import uuid4

from fastapi import Depends, FastAPI, File, HTTPException, Request, UploadFile, WebSocket, WebSocketDisconnect, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field
from pymongo import AsyncMongoClient, ASCENDING, ReturnDocument

from .auction_engine import MAX_ROUNDS, PRESETS, resolve_round, validate_bid
from .auth import current_user, decode_token, issue_token, require_admin
from .config import settings
from .payments import normalize_indian_phone, whatsapp_template_payload


ACTIVE_AUCTION_STATUSES = ("running", "announcing", "starting", "settling")
# A wall clock can be corrected forwards while a local server is running. Keep a
# process-local elapsed-time guard so a newly started round never closes early.
round_started_monotonic: dict[str, tuple[float, int]] = {}
clock_anchor: datetime | None = None
clock_anchor_monotonic = 0.0
clock_lock = asyncio.Lock()


class CreateSale(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    sale_date: str
    description: str | None = Field(default=None, max_length=1000)


class CreateProduct(BaseModel):
    name: str
    brand: str | None = None
    quantity_ml: int = Field(gt=0)
    image_url: str | None = None
    base_price: int = Field(ge=0)
    round_duration_seconds: int = Field(default=30, ge=5, le=600)


class UpdateProduct(BaseModel):
    name: str | None = None
    brand: str | None = None
    quantity_ml: int | None = Field(default=None, gt=0)
    image_url: str | None = None
    base_price: int | None = Field(default=None, ge=0)
    round_duration_seconds: int | None = Field(default=None, ge=5, le=600)


class CorrectProductOutcome(BaseModel):
    status: Literal["sold", "unsold"]
    price: int | None = Field(default=None, ge=0)
    buyer_phone: str | None = Field(default=None, min_length=8, max_length=24)


class ReorderProducts(BaseModel):
    product_ids: list[str]


class CompleteSale(BaseModel):
    force: bool = False


class PauseAuction(BaseModel):
    description: str = Field(min_length=1, max_length=300)


class PaymentSettings(BaseModel):
    qr_url: str = Field(min_length=1, max_length=2000)


class PaymentStatusUpdate(BaseModel):
    payment_status: Literal["paid", "cancelled"]


class UpdatePhone(BaseModel):
    phone: str = Field(min_length=8, max_length=24)


class PresetBid(BaseModel):
    amount: int = Field(ge=0)
    client_bid_id: str = Field(min_length=8, max_length=100)


class GoogleExchange(BaseModel):
    email: str
    name: str | None = None
    image: str | None = None


class Hub:
    def __init__(self): self.connections: dict[str, set[WebSocket]] = {}
    async def join(self, sale_id: str, socket: WebSocket):
        await socket.accept(); self.connections.setdefault(sale_id, set()).add(socket)
    def leave(self, sale_id: str, socket: WebSocket): self.connections.get(sale_id, set()).discard(socket)
    async def publish(self, sale_id: str, payload: dict):
        for socket in list(self.connections.get(sale_id, set())):
            try: await socket.send_json(payload)
            except Exception: self.leave(sale_id, socket)


hub = Hub()


async def create_payment_request(database, result: dict, product: dict, sale_id: str) -> None:
    """Create one durable payment request for an automatic auction winner."""
    if result["status"] != "sold" or not result.get("winner_id") or result.get("manual_override"):
        return
    winner = await database.users.find_one(user_id_filter(result["winner_id"]), {"name": 1, "phone": 1})
    sale = await database.sales.find_one({"_id": sale_id}, {"name": 1})
    settings_doc = await database.settings.find_one({"_id": "payment"}, {"qr_url": 1})
    recipient = normalize_indian_phone(winner.get("phone") if winner else None)
    failure = None
    if not recipient:
        failure = "Winner does not have a valid Indian WhatsApp phone number."
    elif not settings_doc or not settings_doc.get("qr_url"):
        failure = "Payment QR is not configured."
    request = {
        "_id": oid(),
        "result_id": result["_id"],
        "sale_id": sale_id,
        "sale_name": sale.get("name", "Fragrance Bucket sale") if sale else "Fragrance Bucket sale",
        "product_id": product["_id"],
        "product_name": product["name"],
        "amount": result["price"],
        "winner_id": result["winner_id"],
        "winner_name": winner.get("name") if winner else None,
        "recipient": recipient,
        "qr_url": settings_doc.get("qr_url") if settings_doc else None,
        "dispatch_status": "failed" if failure else "queued",
        "delivery_status": None,
        "payment_status": "pending",
        "attempts": 0,
        "last_error": failure,
        "message_id": None,
        "created_at": now(),
        "updated_at": now(),
    }
    try:
        await database.payment_requests.insert_one(request)
    except Exception as error:
        # The unique result index makes creation safe across worker restarts.
        if "duplicate key" not in str(error).lower():
            raise


async def send_payment_request(request: dict) -> tuple[str | None, str | None]:
    if not all((settings.whatsapp_access_token, settings.whatsapp_phone_number_id, settings.whatsapp_template_name)):
        return None, "WhatsApp Cloud API is not configured."
    if not request.get("recipient") or not request.get("qr_url"):
        return None, "A recipient and payment QR are required."
    payload = whatsapp_template_payload(
        recipient=request["recipient"],
        qr_url=request["qr_url"],
        winner_name=request.get("winner_name") or "there",
        product_name=request["product_name"],
        sale_name=request["sale_name"],
        amount=request["amount"],
        template_name=settings.whatsapp_template_name,
    )
    url = f"https://graph.facebook.com/{settings.whatsapp_graph_api_version}/{settings.whatsapp_phone_number_id}/messages"
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.post(url, json=payload, headers={"Authorization": f"Bearer {settings.whatsapp_access_token}"})
    except httpx.HTTPError as error:
        return None, f"WhatsApp request failed: {error}"
    if response.is_error:
        detail = response.json().get("error", {}).get("message", "WhatsApp rejected the request.")
        return None, detail
    return response.json().get("messages", [{}])[0].get("id"), None


async def dispatch_payment_requests(database) -> None:
    while True:
        request = await database.payment_requests.find_one_and_update(
            {"dispatch_status": "queued"},
            {"$set": {"dispatch_status": "sending", "updated_at": now()}},
            return_document=ReturnDocument.BEFORE,
        )
        if not request:
            return
        message_id, error = await send_payment_request(request)
        update = {
            "dispatch_status": "failed" if error else "sent",
            "delivery_status": None if error else "sent",
            "last_error": error,
            "updated_at": now(),
        }
        if message_id:
            update["message_id"] = message_id
        await database.payment_requests.update_one(
            {"_id": request["_id"]},
            {"$set": update, "$inc": {"attempts": 1}},
        )


async def close_expired_rounds(database) -> None:
    """Claim expired rounds before resolving. Safe after restart; only one worker wins claim."""
    while True:
        clock = await database_now(database)
        announced = await database.auctions.find_one_and_update(
            {"status": "announcing", "transition_until": {"$lte": clock}},
            {"$set": {"status": "starting"}},
            return_document=ReturnDocument.BEFORE,
        )
        if announced:
            next_round = announced["pending_round_number"]
            duration = announced.get("round_duration_seconds", 30)
            round_start = await database_now(database)
            await database.auctions.update_one({"_id": announced["_id"]}, {"$set": {"status": "running", "round_number": next_round, "floor": announced["pending_floor"], "eligible_user_ids": announced["pending_eligible_user_ids"], "starts_at": round_start, "ends_at": round_start + timedelta(seconds=duration)}, "$unset": {"pending_round_number": "", "pending_floor": "", "pending_eligible_user_ids": "", "transition_until": "", "transition": ""}})
            round_started_monotonic[announced["_id"]] = (time.monotonic(), duration)
            await hub.publish(announced["sale_id"], {"type": "round.started", "round": next_round})
            continue
        candidate = await database.auctions.find_one(
            {"status": "running", "ends_at": {"$lte": clock}},
            sort=[("ends_at", ASCENDING)],
        )
        if not candidate:
            return
        started = round_started_monotonic.get(candidate["_id"])
        if started and time.monotonic() - started[0] < started[1]:
            # The system clock moved forward. Keep this round open until its
            # configured real elapsed duration has passed.
            return
        auction = await database.auctions.find_one_and_update(
            {"_id": candidate["_id"], "status": "running", "ends_at": {"$lte": clock}},
            {"$set": {"status": "settling"}}, return_document=ReturnDocument.BEFORE,
        )
        if not auction: return
        bids = {bid["user_id"]: bid["amount"] async for bid in database.bids.find({"auction_id": auction["_id"], "round": auction["round_number"]})}
        resolution = resolve_round(round_number=auction["round_number"], bids=bids, prior_tied_users=tuple(auction["eligible_user_ids"]), prior_price=auction["floor"])
        if resolution.outcome == "next_round":
            next_round = auction["round_number"] + 1
            transition_start = await database_now(database)
            await database.auctions.update_one({"_id": auction["_id"], "status": "settling"}, {"$set": {"status": "announcing", "pending_round_number": next_round, "pending_floor": resolution.price, "pending_eligible_user_ids": list(resolution.eligible_user_ids), "transition_until": transition_start + timedelta(seconds=3), "transition": {"participant_count": len(bids), "next_round": next_round}}})
            await hub.publish(auction["sale_id"], {"type": "round.tie"})
            continue
        product_status = "unsold" if resolution.outcome == "unsold" else "sold"
        result = {"auction_id": auction["_id"], "product_id": auction["product_id"], "status": product_status, "price": resolution.price, "winner_id": resolution.winner_id, "rounds_used": auction["round_number"], "final_eligible_user_ids": list(resolution.eligible_user_ids), "random_reason": resolution.random_reason, "completed_at": await database_now(database)}
        result_id = oid()
        result["_id"] = result_id
        await database.auction_results.insert_one(result)
        await database.auctions.update_one({"_id": auction["_id"]}, {"$set": {"status": "completed", "result": result}})
        product = await database.products.find_one({"_id": auction["product_id"]}, {"name": 1, "quantity_ml": 1, "image_url": 1})
        await database.products.update_one({"_id": auction["product_id"]}, {"$set": {"status": product_status}})
        await database.sales.update_one(
            {"_id": auction["sale_id"]},
            {
                "$set": {
                    "last_result": {
                        "product_id": auction["product_id"],
                        "status": product_status,
                        "price": resolution.price,
                        "product_name": product["name"],
                        "quantity_ml": product["quantity_ml"],
                        "winner_id": resolution.winner_id,
                        "random_reason": resolution.random_reason,
                        "tied_count": len(resolution.eligible_user_ids),
                    }
                }
            },
        )
        await database.sales.update_one(
            {"_id": auction["sale_id"], "active_auction_id": auction["_id"]},
            {"$unset": {"active_auction_id": ""}},
        )
        round_started_monotonic.pop(auction["_id"], None)
        await create_payment_request(database, result, product, auction["sale_id"])
        await hub.publish(auction["sale_id"], {"type": "auction.completed"})


async def reconcile_completed_auctions(database) -> None:
    """Repair a completed auction if a reload interrupted its final writes."""
    async for auction in database.auctions.find({"status": "completed"}, {"_id": 1, "sale_id": 1, "product_id": 1}):
        result = await database.auction_results.find_one({"auction_id": auction["_id"]}, {"status": 1})
        if not result:
            continue
        await database.products.update_one(
            {"_id": auction["product_id"], "status": "active"},
            {"$set": {"status": result["status"]}},
        )
        await database.sales.update_one(
            {"_id": auction["sale_id"], "active_auction_id": auction["_id"]},
            {"$unset": {"active_auction_id": ""}},
        )


async def round_worker(database):
    while True:
        await close_expired_rounds(database)
        await dispatch_payment_requests(database)
        await asyncio.sleep(0.5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.mongo = AsyncMongoClient(settings.mongodb_uri)
    app.state.db = app.state.mongo[settings.mongodb_database]
    await app.state.db.command("ping")
    await database_now(app.state.db)
    print(f"MongoDB connected: {settings.mongodb_database}")
    await app.state.db.sales.create_index([("status", ASCENDING)])
    await app.state.db.products.create_index([("sale_id", ASCENDING), ("sequence", ASCENDING)])
    await app.state.db.bids.create_index([("auction_id", ASCENDING), ("round", ASCENDING), ("user_id", ASCENDING)], unique=True)
    await app.state.db.bids.create_index("client_bid_id", unique=True)
    await app.state.db.payment_requests.create_index("result_id", unique=True)
    await app.state.db.payment_requests.create_index([("dispatch_status", ASCENDING), ("created_at", ASCENDING)])
    await reconcile_completed_auctions(app.state.db)
    worker = asyncio.create_task(round_worker(app.state.db))
    yield
    worker.cancel()
    await app.state.mongo.close()


app = FastAPI(title="Fragrance Bucket API", lifespan=lifespan)
cors_origins = [
    origin.strip().rstrip("/")
    for origin in settings.web_origin.split(",")
    if origin.strip()
]
production_web_origin = "https://auction.fragrancebucket.com"
if production_web_origin not in cors_origins:
    cors_origins.append(production_web_origin)
print(f"CORS allowed origins: {', '.join(cors_origins)}")
app.add_middleware(CORSMiddleware, allow_origins=cors_origins, allow_credentials=True, allow_methods=["*"], allow_headers=["*"])


@app.get("/health", include_in_schema=False)
async def healthcheck():
    return {"status": "ok"}


def db(): return app.state.db
def now(): return datetime.now(timezone.utc)
def oid(): return str(uuid4())
def as_utc(value: datetime) -> datetime: return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
def utc_iso(value: datetime) -> str: return as_utc(value).isoformat()
async def database_now(database) -> datetime:
    """Use one calibrated MongoDB clock without a remote call on every poll."""
    global clock_anchor, clock_anchor_monotonic
    elapsed = time.monotonic() - clock_anchor_monotonic
    if clock_anchor is not None and elapsed < 15:
        return clock_anchor + timedelta(seconds=elapsed)
    async with clock_lock:
        elapsed = time.monotonic() - clock_anchor_monotonic
        if clock_anchor is None or elapsed >= 15:
            clock_anchor = as_utc((await database.command("hello"))["localTime"])
            clock_anchor_monotonic = time.monotonic()
        return clock_anchor + timedelta(seconds=time.monotonic() - clock_anchor_monotonic)
def user_id_filter(user_id: str) -> dict: return {"$expr": {"$eq": [{"$toString": "$_id"}, user_id]}}


async def customer_snapshot(database, sale_id: str, user_id: str | None = None) -> dict:
    clock = await database_now(database)
    sale = await database.sales.find_one({"_id": sale_id}, {"name": 1, "status": 1, "last_result": 1, "is_paused": 1, "pause_description": 1})
    if not sale: raise HTTPException(404, "Sale not found.")
    completed_auctions = [auction async for auction in database.auctions.find({"sale_id": sale_id, "status": "completed"}, {"_id": 1, "product_id": 1})]
    completed_auction_ids = [auction["_id"] for auction in completed_auctions]
    completed_results = [result async for result in database.auction_results.find({"auction_id": {"$in": completed_auction_ids}}).sort("completed_at", -1)] if completed_auction_ids else []
    completed_product_ids = [result["product_id"] for result in completed_results]
    completed_products = {product["_id"]: product async for product in database.products.find({"_id": {"$in": completed_product_ids}}, {"name": 1, "quantity_ml": 1, "image_url": 1})} if completed_product_ids else {}
    past_results = [
        {
            "productId": result["product_id"],
            "name": completed_products.get(result["product_id"], {}).get("name", "Completed product"),
            "quantityMl": completed_products.get(result["product_id"], {}).get("quantity_ml"),
            "imageUrl": completed_products.get(result["product_id"], {}).get("image_url"),
            "status": result["status"],
            "price": result.get("price"),
            "completedAt": utc_iso(result["completed_at"]),
        }
        for result in completed_results
    ]
    active = await database.auctions.find_one({"sale_id": sale_id, "status": {"$in": ACTIVE_AUCTION_STATUSES}})
    last = sale.get("last_result")
    visible_last = None
    if last:
        visible_last = {"status": last["status"], "price": last.get("price"), "productName": last.get("product_name"), "quantityMl": last.get("quantity_ml"), "myOutcome": "won" if last.get("winner_id") == user_id else "lost", "randomReason": last.get("random_reason"), "tiedCount": last.get("tied_count", 0)}
    next_product = None
    if sale["status"] == "live":
        prepared_product = await database.products.find_one(
            {"sale_id": sale_id, "status": "prepared"},
            {"name": 1, "quantity_ml": 1, "image_url": 1},
            sort=[("sequence", ASCENDING)],
        )
        if prepared_product:
            next_product = {
                "name": prepared_product["name"],
                "quantityMl": prepared_product["quantity_ml"],
                "imageUrl": prepared_product.get("image_url"),
            }
    result = {"saleId": sale_id, "saleName": sale["name"], "saleStatus": sale["status"], "serverNow": utc_iso(clock), "lastResult": visible_last, "pastResults": past_results, "nextProduct": next_product}
    if sale.get("is_paused"):
        result.update({"state": "paused", "pause": {"description": sale.get("pause_description", "The sale is taking a short break.")}})
        return result
    if not active:
        result["state"] = "completed" if sale["status"] == "completed" else "waiting"
        return result
    product = await database.products.find_one({"_id": active["product_id"]}, {"name": 1, "quantity_ml": 1, "base_price": 1, "image_url": 1})
    if active["status"] in ("announcing", "starting"):
        result.update({"state": "transition", "transition": {"product": product, "nextRound": active["transition"]["next_round"], "participantCount": active["transition"]["participant_count"], "endsAt": utc_iso(active["transition_until"])}})
        return result
    if active["status"] == "settling":
        result.update({"state": "settling", "auction": {"product": product}})
        return result
    personal_bid = None
    if user_id:
        bid = await database.bids.find_one({"auction_id": active["_id"], "round": active["round_number"], "user_id": user_id})
        personal_bid = bid and bid["amount"]
    participant_count = await database.bids.count_documents({"auction_id": active["_id"], "round": active["round_number"]})
    result.update({"state": "running", "auction": {"id": active["_id"], "round": active["round_number"], "endsAt": utc_iso(active["ends_at"]), "floor": active["floor"], "product": product, "myBid": personal_bid, "participantCount": participant_count, "eligible": True}})
    return result


@app.post("/admin/sales", status_code=201)
async def create_sale(payload: CreateSale, _: dict = Depends(require_admin), database=Depends(db)):
    sale = {"_id": oid(), **payload.model_dump(), "status": "draft", "created_at": now()}
    await database.sales.insert_one(sale); return sale


@app.get("/admin/sales")
async def list_sales(_: dict = Depends(require_admin), database=Depends(db)):
    sales = []
    async for sale in database.sales.find({}, {"name": 1, "sale_date": 1, "status": 1}).sort("sale_date", -1):
        sale["product_count"] = await database.products.count_documents({"sale_id": sale["_id"]})
        sales.append(sale)
    return sales


@app.get("/admin/users")
async def list_users(_: dict = Depends(require_admin), database=Depends(db)):
    users = []
    async for user in database.users.find({}, {"name": 1, "email": 1, "phone": 1, "role": 1, "created_at": 1, "last_login_at": 1}).sort("created_at", -1):
        users.append({
            "id": str(user["_id"]),
            "name": user.get("name"),
            "email": user.get("email"),
            "phone": user.get("phone"),
            "role": user.get("role", "customer"),
            "createdAt": utc_iso(user["created_at"]) if user.get("created_at") else None,
            "lastLoginAt": utc_iso(user["last_login_at"]) if user.get("last_login_at") else None,
        })
    return users


@app.get("/admin/sales/{sale_id}")
async def admin_sale_detail(sale_id: str, _: dict = Depends(require_admin), database=Depends(db)):
    sale = await database.sales.find_one({"_id": sale_id})
    if not sale: raise HTTPException(404, "Sale not found.")
    sale["products"] = [product async for product in database.products.find({"sale_id": sale_id}).sort("sequence", ASCENDING)]
    sale["active_auction"] = await database.auctions.find_one(
        {"sale_id": sale_id, "status": {"$in": ACTIVE_AUCTION_STATUSES}}
    )
    if sale["active_auction"]:
        sale["active_auction"]["ends_at"] = utc_iso(sale["active_auction"]["ends_at"])
    auctions = [auction async for auction in database.auctions.find({"sale_id": sale_id}, {"_id": 1, "product_id": 1})]
    auction_ids = [auction["_id"] for auction in auctions]
    products_by_id = {product["_id"]: product for product in sale["products"]}
    product_by_auction = {auction["_id"]: products_by_id.get(auction["product_id"], {}) for auction in auctions}
    sale["results"] = [result async for result in database.auction_results.find({"auction_id": {"$in": auction_ids}}).sort("completed_at", -1)]
    winner_ids = list({result["winner_id"] for result in sale["results"] if result.get("winner_id")})
    users = {
        str(candidate["_id"]): candidate
        async for candidate in database.users.find(
            {"$expr": {"$in": [{"$toString": "$_id"}, winner_ids]}},
            {"email": 1, "name": 1, "phone": 1},
        )
    } if winner_ids else {}
    for result in sale["results"]:
        result["product"] = products_by_id.get(result["product_id"])
        if result.get("winner_id"):
            candidate = users.get(result["winner_id"])
            if candidate: result["winner"] = {"email": candidate.get("email"), "name": candidate.get("name"), "phone": candidate.get("phone")}
    return sale


@app.post("/auth/google/exchange")
async def exchange_google_identity(payload: GoogleExchange, x_exchange_secret: str | None = __import__("fastapi").Header(default=None), database=Depends(db)):
    if not x_exchange_secret or not secrets.compare_digest(x_exchange_secret, settings.google_exchange_secret):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid identity exchange.")
    email = payload.email.lower().strip()
    user = await database.users.find_one_and_update(
        {"email": email},
        {"$set": {"email": email, "name": payload.name, "image": payload.image, "last_login_at": now()}, "$setOnInsert": {"role": "customer", "created_at": now()}},
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    return {"accessToken": issue_token(str(user["_id"]), user["role"]), "role": user["role"]}


@app.post("/admin/sales/{sale_id}/products", status_code=201)
async def add_product(sale_id: str, payload: CreateProduct, _: dict = Depends(require_admin), database=Depends(db)):
    sale = await database.sales.find_one({"_id": sale_id})
    if not sale or sale["status"] not in ("draft", "ready"): raise HTTPException(409, "Sale cannot be edited.")
    sequence = await database.products.count_documents({"sale_id": sale_id})
    product = {"_id": oid(), "sale_id": sale_id, **payload.model_dump(), "sequence": sequence, "status": "prepared"}
    await database.products.insert_one(product); return product


@app.post("/admin/uploads/images")
async def upload_product_image(file: UploadFile = File(...), _: dict = Depends(require_admin)):
    if not settings.imagekit_private_key or not settings.imagekit_public_key:
        raise HTTPException(503, "ImageKit is not configured.")
    if not file.content_type or not file.content_type.startswith("image/"):
        raise HTTPException(422, "Upload an image file.")
    content = await file.read()
    if len(content) > 10 * 1024 * 1024:
        raise HTTPException(422, "Image must be 10 MB or smaller.")
    filename = f"{uuid4()}-{file.filename or 'product-image'}"
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post("https://upload.imagekit.io/api/v1/files/upload", auth=(settings.imagekit_private_key, ""), data={"fileName": filename, "folder": "/fragrance-bucket/products"}, files={"file": (file.filename or "product-image", content, file.content_type)})
    if response.is_error:
        raise HTTPException(502, "Image upload failed.")
    return {"url": response.json()["url"]}


@app.get("/admin/payment-settings")
async def get_payment_settings(_: dict = Depends(require_admin), database=Depends(db)):
    payment = await database.settings.find_one({"_id": "payment"}, {"qr_url": 1, "updated_at": 1})
    return {"qr_url": payment.get("qr_url") if payment else None, "configured": bool(payment and payment.get("qr_url"))}


@app.put("/admin/payment-settings")
async def update_payment_settings(payload: PaymentSettings, _: dict = Depends(require_admin), database=Depends(db)):
    await database.settings.update_one(
        {"_id": "payment"},
        {"$set": {"qr_url": payload.qr_url.strip(), "updated_at": now()}},
        upsert=True,
    )
    return {"qr_url": payload.qr_url.strip(), "configured": True}


@app.post("/admin/payment-requests/{request_id}/resend")
async def resend_payment_request(request_id: str, _: dict = Depends(require_admin), database=Depends(db)):
    settings_doc = await database.settings.find_one({"_id": "payment"}, {"qr_url": 1})
    if not settings_doc or not settings_doc.get("qr_url"):
        raise HTTPException(409, "Configure a payment QR before resending.")
    request = await database.payment_requests.find_one_and_update(
        {"_id": request_id, "payment_status": "pending"},
        {"$set": {"dispatch_status": "queued", "delivery_status": None, "last_error": None, "qr_url": settings_doc["qr_url"], "updated_at": now()}},
        return_document=ReturnDocument.AFTER,
    )
    if not request:
        raise HTTPException(409, "Only pending payment requests can be resent.")
    return {"ok": True, "dispatch_status": request["dispatch_status"]}


@app.patch("/admin/payment-requests/{request_id}/payment-status")
async def update_payment_status(request_id: str, payload: PaymentStatusUpdate, _: dict = Depends(require_admin), database=Depends(db)):
    request = await database.payment_requests.find_one_and_update(
        {"_id": request_id},
        {"$set": {"payment_status": payload.payment_status, "updated_at": now()}},
        return_document=ReturnDocument.AFTER,
    )
    if not request:
        raise HTTPException(404, "Payment request not found.")
    return {"ok": True, "payment_status": request["payment_status"]}


@app.get("/webhooks/whatsapp")
async def verify_whatsapp_webhook(request: Request):
    query = request.query_params
    if settings.whatsapp_verify_token and query.get("hub.mode") == "subscribe" and hmac.compare_digest(query.get("hub.verify_token", ""), settings.whatsapp_verify_token):
        return PlainTextResponse(query.get("hub.challenge", ""))
    raise HTTPException(403, "Webhook verification failed.")


@app.post("/webhooks/whatsapp")
async def receive_whatsapp_webhook(request: Request, database=Depends(db)):
    raw_body = await request.body()
    if settings.whatsapp_app_secret:
        provided = request.headers.get("X-Hub-Signature-256", "")
        expected = "sha256=" + hmac.new(settings.whatsapp_app_secret.encode(), raw_body, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(provided, expected):
            raise HTTPException(403, "Invalid webhook signature.")
    payload = await request.json()
    for entry in payload.get("entry", []):
        for change in entry.get("changes", []):
            for status_event in change.get("value", {}).get("statuses", []):
                message_id = status_event.get("id")
                delivery_status = status_event.get("status")
                if message_id and delivery_status:
                    await database.payment_requests.update_one(
                        {"message_id": message_id},
                        {"$set": {"delivery_status": delivery_status, "updated_at": now()}},
                    )
    return {"ok": True}


@app.patch("/admin/products/{product_id}")
async def edit_product(product_id: str, payload: UpdateProduct, _: dict = Depends(require_admin), database=Depends(db)):
    product = await database.products.find_one({"_id": product_id})
    if not product: raise HTTPException(404, "Product not found.")
    sale = await database.sales.find_one({"_id": product["sale_id"]})
    if sale["status"] == "completed" or product["status"] != "prepared": raise HTTPException(409, "Only upcoming products can be edited.")
    updated = await database.products.find_one_and_update({"_id": product_id}, {"$set": payload.model_dump(exclude_unset=True)}, return_document=ReturnDocument.AFTER)
    return updated


@app.patch("/admin/products/{product_id}/outcome")
async def correct_product_outcome(product_id: str, payload: CorrectProductOutcome, _: dict = Depends(require_admin), database=Depends(db)):
    product = await database.products.find_one({"_id": product_id})
    if not product: raise HTTPException(404, "Product not found.")
    if product["status"] not in ("sold", "unsold"):
        raise HTTPException(409, "Only completed product outcomes can be corrected.")
    result = await database.auction_results.find_one({"product_id": product_id})
    if not result: raise HTTPException(409, "No completed auction result found for this product.")
    if payload.status == "sold" and (payload.price is None or not (payload.buyer_phone or "").strip()):
        raise HTTPException(422, "A sale price and buyer phone number are required when marking a product sold.")

    outcome_update = {
        "status": payload.status,
        "price": payload.price if payload.status == "sold" else None,
        "winner_id": None,
        "manual_buyer_phone": payload.buyer_phone.strip() if payload.status == "sold" and payload.buyer_phone else None,
        "manual_override": True,
        "corrected_at": now(),
    }
    await database.auction_results.update_one({"_id": result["_id"]}, {"$set": outcome_update})
    await database.products.update_one({"_id": product_id}, {"$set": {"status": payload.status}})

    sale = await database.sales.find_one({"_id": product["sale_id"]}, {"last_result": 1})
    last_result = sale and sale.get("last_result")
    if last_result and (last_result.get("product_id") == product_id or (last_result.get("product_name") == product["name"] and last_result.get("quantity_ml") == product["quantity_ml"])):
        await database.sales.update_one(
            {"_id": product["sale_id"]},
            {"$set": {"last_result": {
                "product_id": product_id,
                "status": payload.status,
                "price": outcome_update["price"],
                "product_name": product["name"],
                "quantity_ml": product["quantity_ml"],
                "winner_id": None,
                "manual_buyer_phone": outcome_update["manual_buyer_phone"],
                "random_reason": None,
                "tied_count": 0,
            }}},
        )
    return {"ok": True, "status": payload.status}


@app.get("/admin/products/{product_id}")
async def admin_product_detail(product_id: str, _: dict = Depends(require_admin), database=Depends(db)):
    product = await database.products.find_one({"_id": product_id})
    if not product: raise HTTPException(404, "Product not found.")
    auction = await database.auctions.find_one({"product_id": product_id})
    result = await database.auction_results.find_one({"product_id": product_id})
    response = {"product": product, "auction": auction, "result": result}
    if result and result["status"] == "sold":
        users = {str(candidate["_id"]): candidate async for candidate in database.users.find({}, {"name": 1, "email": 1, "phone": 1})}
        winner = users.get(result.get("winner_id", ""))
        response["winner"] = {"name": winner.get("name"), "email": winner.get("email"), "phone": winner.get("phone")} if winner else None
        response["manual_buyer_phone"] = result.get("manual_buyer_phone")
        response["bid_history"] = [{"round": bid["round"], "amount": bid["amount"], "submitted_at": utc_iso(bid["submitted_at"]), "bidder": {"name": users.get(bid["user_id"], {}).get("name"), "email": users.get(bid["user_id"], {}).get("email")}} async for bid in database.bids.find({"auction_id": auction["_id"]}).sort("submitted_at", -1)] if auction else []
        payment_request = await database.payment_requests.find_one({"result_id": result["_id"]})
        if payment_request:
            response["payment_request"] = {
                "id": payment_request["_id"],
                "recipient": payment_request.get("recipient"),
                "amount": payment_request.get("amount"),
                "qr_url": payment_request.get("qr_url"),
                "dispatch_status": payment_request.get("dispatch_status"),
                "delivery_status": payment_request.get("delivery_status"),
                "payment_status": payment_request.get("payment_status"),
                "attempts": payment_request.get("attempts", 0),
                "last_error": payment_request.get("last_error"),
            }
    return response


@app.get("/admin/sales/{sale_id}/live-dashboard")
async def live_dashboard(sale_id: str, _: dict = Depends(require_admin), database=Depends(db)):
    sale = await database.sales.find_one({"_id": sale_id}, {"name": 1, "status": 1, "last_result": 1, "is_paused": 1, "pause_description": 1})
    if not sale: raise HTTPException(404, "Sale not found.")
    products = [product async for product in database.products.find({"sale_id": sale_id}, {"name": 1, "status": 1})]
    products_by_id = {product["_id"]: product for product in products}
    counts = {status: sum(product["status"] == status for product in products) for status in ("prepared", "active", "sold", "unsold", "skipped")}
    auction = await database.auctions.find_one({"sale_id": sale_id, "status": {"$in": ["running", "announcing", "settling"]}})
    all_auctions = [auction async for auction in database.auctions.find({"sale_id": sale_id}, {"_id": 1})]
    auction_ids = [item["_id"] for item in all_auctions]
    results = [result async for result in database.auction_results.find({"auction_id": {"$in": auction_ids}}).sort("completed_at", -1)] if auction_ids else []
    bids = [bid async for bid in database.bids.find({"auction_id": auction["_id"], "round": auction["round_number"]}).sort("amount", -1)] if auction else []
    user_ids = list({
        *[result["winner_id"] for result in results if result.get("winner_id")],
        *[bid["user_id"] for bid in bids],
    })
    users = {
        str(user["_id"]): user
        async for user in database.users.find(
            {"$expr": {"$in": [{"$toString": "$_id"}, user_ids]}},
            {"name": 1, "email": 1, "phone": 1},
        )
    } if user_ids else {}
    past_results = [{"productName": products_by_id.get(result["product_id"], {}).get("name", "Deleted product"), "status": result["status"], "price": result.get("price"), "winner": {"name": users.get(result.get("winner_id", ""), {}).get("name"), "email": users.get(result.get("winner_id", ""), {}).get("email"), "phone": users.get(result.get("winner_id", ""), {}).get("phone")}} for result in results]
    dashboard = {"saleName": sale["name"], "saleStatus": sale["status"], "salePaused": sale.get("is_paused", False), "pauseDescription": sale.get("pause_description"), "liveConnections": len(hub.connections.get(sale_id, set())), "progress": counts, "auction": None, "bids": [], "lastResult": sale.get("last_result"), "pastResults": past_results}
    if not auction: return dashboard
    product = await database.products.find_one({"_id": auction["product_id"]}, {"name": 1, "quantity_ml": 1, "base_price": 1, "image_url": 1})
    dashboard["auction"] = {"id": auction["_id"], "status": auction["status"], "round": auction["round_number"], "endsAt": utc_iso(auction["ends_at"]) if auction.get("ends_at") else None, "floor": auction["floor"], "product": product, "participantCount": len(bids)}
    dashboard["bids"] = [{"amount": bid["amount"], "updates": len(bid.get("history", [])), "submittedAt": utc_iso(bid["submitted_at"]), "bidder": {"name": users.get(bid["user_id"], {}).get("name"), "email": users.get(bid["user_id"], {}).get("email")}} for bid in bids]
    return dashboard


@app.post("/admin/products/{product_id}/duplicate", status_code=201)
async def duplicate_product(product_id: str, _: dict = Depends(require_admin), database=Depends(db)):
    product = await database.products.find_one({"_id": product_id})
    if not product: raise HTTPException(404, "Product not found.")
    sale = await database.sales.find_one({"_id": product["sale_id"]})
    if sale["status"] not in ("draft", "ready") or product["status"] != "prepared": raise HTTPException(409, "Only prepared products can be duplicated.")
    clone = {key: value for key, value in product.items() if key not in ("_id", "sequence")}
    clone.update({"_id": oid(), "sequence": await database.products.count_documents({"sale_id": product["sale_id"]}), "status": "prepared"})
    await database.products.insert_one(clone); return clone


@app.post("/admin/sales/{sale_id}/reorder")
async def reorder_products(sale_id: str, payload: ReorderProducts, _: dict = Depends(require_admin), database=Depends(db)):
    sale = await database.sales.find_one({"_id": sale_id})
    if not sale or sale["status"] not in ("draft", "ready"): raise HTTPException(409, "Sale cannot be reordered.")
    products = [product async for product in database.products.find({"sale_id": sale_id, "status": "prepared"})]
    if set(payload.product_ids) != {product["_id"] for product in products}: raise HTTPException(422, "Order must contain every prepared product exactly once.")
    for sequence, product_id in enumerate(payload.product_ids): await database.products.update_one({"_id": product_id}, {"$set": {"sequence": sequence}})
    return {"ok": True}


@app.post("/admin/products/{product_id}/skip")
async def skip_product(product_id: str, _: dict = Depends(require_admin), database=Depends(db)):
    product = await database.products.find_one({"_id": product_id, "status": "prepared"})
    if not product: raise HTTPException(409, "Only prepared products can be skipped.")
    await database.products.update_one({"_id": product_id}, {"$set": {"status": "skipped"}})
    return {"ok": True}


@app.delete("/admin/products/{product_id}")
async def delete_product(product_id: str, _: dict = Depends(require_admin), database=Depends(db)):
    product = await database.products.find_one({"_id": product_id})
    if not product: raise HTTPException(404, "Product not found.")
    sale = await database.sales.find_one({"_id": product["sale_id"]})
    if not sale or sale["status"] == "completed": raise HTTPException(409, "Products cannot be deleted from a completed sale.")
    if product["status"] not in ("prepared", "skipped"):
        raise HTTPException(409, "Active or completed products cannot be deleted.")
    await database.products.delete_one({"_id": product_id})
    return {"ok": True}


@app.post("/admin/sales/{sale_id}/ready")
async def mark_ready(sale_id: str, _: dict = Depends(require_admin), database=Depends(db)):
    changed = await database.sales.find_one_and_update({"_id": sale_id, "status": "draft"}, {"$set": {"status": "ready"}}, return_document=ReturnDocument.AFTER)
    if not changed: raise HTTPException(409, "Only draft sales can become ready.")
    return changed


@app.post("/admin/sales/{sale_id}/start")
async def start_sale(sale_id: str, _: dict = Depends(require_admin), database=Depends(db)):
    changed = await database.sales.find_one_and_update({"_id": sale_id, "status": "ready"}, {"$set": {"status": "live"}}, return_document=ReturnDocument.AFTER)
    if not changed: raise HTTPException(409, "Only ready sales can start.")
    await hub.publish(sale_id, {"type": "sale.live"}); return changed


@app.post("/admin/sales/{sale_id}/complete")
async def complete_sale(sale_id: str, payload: CompleteSale, _: dict = Depends(require_admin), database=Depends(db)):
    if await database.auctions.find_one({"sale_id": sale_id, "status": {"$in": ACTIVE_AUCTION_STATUSES}}): raise HTTPException(409, "Cannot complete sale during an active auction.")
    pending = await database.products.count_documents({"sale_id": sale_id, "status": "prepared"})
    if pending and not payload.force: raise HTTPException(409, {"message": "Products remain unprocessed.", "pending": pending})
    changed = await database.sales.find_one_and_update({"_id": sale_id, "status": "live"}, {"$set": {"status": "completed", "completed_at": now()}}, return_document=ReturnDocument.AFTER)
    if not changed: raise HTTPException(409, "Only live sales can be completed.")
    await hub.publish(sale_id, {"type": "sale.completed"}); return changed


@app.post("/admin/sales/{sale_id}/pause")
async def pause_sale(sale_id: str, payload: PauseAuction, _: dict = Depends(require_admin), database=Depends(db)):
    if await database.auctions.find_one({"sale_id": sale_id, "status": {"$in": ACTIVE_AUCTION_STATUSES}}):
        raise HTTPException(409, "Finish the current product auction before pausing the sale.")
    sale = await database.sales.find_one_and_update(
        {"_id": sale_id, "status": "live", "is_paused": {"$ne": True}},
        {"$set": {"is_paused": True, "pause_description": payload.description.strip(), "paused_at": now()}},
        return_document=ReturnDocument.AFTER,
    )
    if not sale: raise HTTPException(409, "Only an unpaused live sale can be paused.")
    await hub.publish(sale_id, {"type": "sale.paused"})
    return {"ok": True}


@app.post("/admin/sales/{sale_id}/resume")
async def resume_sale(sale_id: str, _: dict = Depends(require_admin), database=Depends(db)):
    sale = await database.sales.find_one_and_update(
        {"_id": sale_id, "status": "live", "is_paused": True},
        {"$set": {"is_paused": False}, "$unset": {"pause_description": "", "paused_at": ""}},
        return_document=ReturnDocument.AFTER,
    )
    if not sale: raise HTTPException(409, "No paused live sale found.")
    await hub.publish(sale_id, {"type": "sale.resumed"})
    return {"ok": True}


@app.post("/admin/sales/{sale_id}/start-next")
async def start_next(sale_id: str, _: dict = Depends(require_admin), database=Depends(db)):
    sale = await database.sales.find_one({"_id": sale_id, "status": "live", "is_paused": {"$ne": True}})
    if not sale: raise HTTPException(409, "Sale is not live or is on a break.")
    active_states = ACTIVE_AUCTION_STATUSES
    # Compatibility recovery for locks written by an interrupted/reloaded local
    # server. A lock is only removed when its own auction is no longer active.
    locked_auction_id = sale.get("active_auction_id")
    if locked_auction_id:
        locked_auction = await database.auctions.find_one(
            {"_id": locked_auction_id, "status": {"$in": active_states}},
            {"_id": 1},
        )
        if locked_auction:
            raise HTTPException(409, "An auction is already active.")
        await database.sales.update_one(
            {"_id": sale_id, "active_auction_id": locked_auction_id},
            {"$unset": {"active_auction_id": ""}},
        )
    if await database.auctions.find_one({"sale_id": sale_id, "status": {"$in": active_states}}): raise HTTPException(409, "An auction is already active.")
    product = await database.products.find_one({"sale_id": sale_id, "status": "prepared"}, sort=[("sequence", ASCENDING)])
    if not product: raise HTTPException(409, "No prepared product remains.")
    duration = product.get("round_duration_seconds", 30)
    auction_id = oid()
    # This atomic claim prevents double-clicks or concurrent admins from launching
    # multiple prepared products in the same sale.
    claimed_sale = await database.sales.find_one_and_update(
        {"_id": sale_id, "status": "live", "is_paused": {"$ne": True}, "active_auction_id": {"$exists": False}},
        {"$set": {"active_auction_id": auction_id}},
        return_document=ReturnDocument.AFTER,
    )
    if not claimed_sale:
        raise HTTPException(409, "An auction is already being started or is active.")
    claimed_product = await database.products.find_one_and_update(
        {"_id": product["_id"], "status": "prepared"},
        {"$set": {"status": "active"}},
        return_document=ReturnDocument.AFTER,
    )
    if not claimed_product:
        await database.sales.update_one({"_id": sale_id, "active_auction_id": auction_id}, {"$unset": {"active_auction_id": ""}})
        raise HTTPException(409, "That product is no longer available.")
    started_at = await database_now(database)
    auction = {"_id": auction_id, "sale_id": sale_id, "product_id": product["_id"], "status": "running", "round_number": 1, "floor": product["base_price"], "round_duration_seconds": duration, "eligible_user_ids": [], "starts_at": started_at, "ends_at": started_at + timedelta(seconds=duration)}
    try:
        await database.auctions.insert_one(auction)
    except Exception:
        await database.products.update_one({"_id": product["_id"], "status": "active"}, {"$set": {"status": "prepared"}})
        await database.sales.update_one({"_id": sale_id, "active_auction_id": auction_id}, {"$unset": {"active_auction_id": ""}})
        raise
    round_started_monotonic[auction_id] = (time.monotonic(), duration)
    await hub.publish(sale_id, {"type": "auction.started"}); return auction


@app.get("/sales/live")
async def live_sales(_: dict = Depends(current_user), database=Depends(db)):
    return [
        {"id": sale["_id"], "name": sale["name"], "saleDate": sale.get("sale_date")}
        async for sale in database.sales.find({"status": "live"}, {"name": 1, "sale_date": 1}).sort("sale_date", ASCENDING)
    ]


@app.get("/sales/{sale_id}")
async def get_sale(sale_id: str, user: dict = Depends(current_user), database=Depends(db)):
    return await customer_snapshot(database, sale_id, user["sub"])


@app.get("/me")
async def get_me(user: dict = Depends(current_user), database=Depends(db)):
    profile = await database.users.find_one(
        user_id_filter(user["sub"]), {"name": 1, "email": 1, "image": 1, "phone": 1}
    )
    if not profile:
        raise HTTPException(401, "User profile not found.")
    return {
        "name": profile.get("name"),
        "email": profile.get("email"),
        "image": profile.get("image"),
        "phone": profile.get("phone"),
        "hasPhone": bool(profile.get("phone")),
    }


@app.put("/me/phone")
async def update_phone(payload: UpdatePhone, user: dict = Depends(current_user), database=Depends(db)):
    phone = re.sub(r"[\s()-]", "", payload.phone)
    if not re.fullmatch(r"\+?[1-9]\d{7,14}", phone):
        raise HTTPException(422, "Enter a valid phone number, including country code if needed.")
    updated = await database.users.update_one(user_id_filter(user["sub"]), {"$set": {"phone": phone, "phone_updated_at": now()}})
    if not updated.matched_count: raise HTTPException(401, "User profile not found.")
    return {"hasPhone": True}


@app.get("/me/auctions")
async def my_auctions(user: dict = Depends(current_user), database=Depends(db)):
    bids = [bid async for bid in database.bids.find({"user_id": user["sub"]}, {"auction_id": 1, "amount": 1})]
    auction_ids = list({bid["auction_id"] for bid in bids})
    auctions = {auction["_id"]: auction async for auction in database.auctions.find({"_id": {"$in": auction_ids}})}
    results = {result["auction_id"]: result async for result in database.auction_results.find({"auction_id": {"$in": auction_ids}})}
    records = []
    for auction_id in auction_ids:
        auction = auctions.get(auction_id)
        if not auction: continue
        product = await database.products.find_one({"_id": auction["product_id"]}, {"name": 1, "quantity_ml": 1})
        result = results.get(auction_id)
        state = "participating" if not result else ("won" if result.get("winner_id") == user["sub"] else "lost")
        completed = as_utc(result["completed_at"]) if result else None
        records.append({"auctionId": auction_id, "status": state, "product": product, "myHighestBid": max(bid["amount"] for bid in bids if bid["auction_id"] == auction_id), "finalPrice": result and result.get("price"), "completedAt": utc_iso(completed) if completed else None, "sortTime": completed})
    records.sort(key=lambda record: record["sortTime"] or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    for record in records: record.pop("sortTime")
    return records


@app.post("/auctions/{auction_id}/bids")
async def submit_bid(auction_id: str, payload: PresetBid, user: dict = Depends(current_user), database=Depends(db)):
    auction = await database.auctions.find_one({"_id": auction_id, "status": "running"})
    if not auction or as_utc(auction["ends_at"]) <= await database_now(database): raise HTTPException(409, "This round has ended.")
    profile = await database.users.find_one(user_id_filter(user["sub"]), {"phone": 1})
    if not profile or not profile.get("phone"): raise HTTPException(403, "Add your phone number before placing a bid.")
    prior = await database.bids.find_one({"auction_id": auction_id, "round": auction["round_number"], "user_id": user["sub"]})
    existing_request = await database.bids.find_one({"$or": [{"client_bid_id": payload.client_bid_id}, {"client_bid_ids": payload.client_bid_id}]})
    if existing_request: return {"accepted": True, "amount": existing_request["amount"], "idempotent": True}
    amount = payload.amount
    difference = amount - auction["floor"]
    if difference != 0 and (difference < min(PRESETS) or difference % 5 != 0):
        raise HTTPException(422, "Choose a valid preset amount.")
    try: validate_bid(amount=amount, floor=auction["floor"], previous_amount=prior and prior["amount"])
    except ValueError as error: raise HTTPException(422, str(error)) from error
    if prior:
        await database.bids.update_one({"_id": prior["_id"], "amount": {"$lte": amount}}, {"$set": {"amount": amount, "submitted_at": now()}, "$push": {"history": {"amount": amount, "client_bid_id": payload.client_bid_id, "submitted_at": now()}, "client_bid_ids": payload.client_bid_id}})
    else:
        bid = {"_id": oid(), "auction_id": auction_id, "round": auction["round_number"], "user_id": user["sub"], "amount": amount, "client_bid_id": payload.client_bid_id, "client_bid_ids": [], "history": [{"amount": amount, "client_bid_id": payload.client_bid_id, "submitted_at": now()}], "submitted_at": now()}
        await database.bids.insert_one(bid)
    await hub.publish(auction["sale_id"], {"type": "bid.accepted"})
    return {"accepted": True, "amount": amount, "idempotent": False}


@app.websocket("/ws/sales/{sale_id}")
async def sale_socket(socket: WebSocket, sale_id: str, token: str | None = None):
    try:
        if not token: raise ValueError
        decode_token(token)
    except Exception:
        await socket.close(code=status.WS_1008_POLICY_VIOLATION)
        return
    await hub.join(sale_id, socket)
    try:
        while True: await socket.receive_text()
    except WebSocketDisconnect: hub.leave(sale_id, socket)
