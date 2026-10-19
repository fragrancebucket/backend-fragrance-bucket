import re


def normalize_indian_phone(value: str | None) -> str | None:
    """Return Meta's digits-only Indian WhatsApp recipient format."""
    digits = re.sub(r"\D", "", value or "")
    if len(digits) == 10 and digits[0] in "6789":
        return f"91{digits}"
    if len(digits) == 12 and digits.startswith("91") and digits[2] in "6789":
        return digits
    return None


def whatsapp_template_payload(*, recipient: str, qr_url: str, winner_name: str, product_name: str, sale_name: str, amount: int, template_name: str) -> dict:
    return {
        "messaging_product": "whatsapp",
        "to": recipient,
        "type": "template",
        "template": {
            "name": template_name,
            "language": {"code": "en"},
            "components": [
                {"type": "header", "parameters": [{"type": "image", "image": {"link": qr_url}}]},
                {"type": "body", "parameters": [
                    {"type": "text", "text": winner_name or "there"},
                    {"type": "text", "text": product_name},
                    {"type": "text", "text": f"₹{amount}"},
                    {"type": "text", "text": sale_name},
                ]},
            ],
        },
    }
