from app.payments import normalize_indian_phone, whatsapp_template_payload


def test_normalize_indian_phone_accepts_local_and_country_code():
    assert normalize_indian_phone("98765 43210") == "919876543210"
    assert normalize_indian_phone("+91 98765-43210") == "919876543210"


def test_normalize_indian_phone_rejects_invalid_values():
    assert normalize_indian_phone("12345") is None
    assert normalize_indian_phone("+1 202 555 0100") is None


def test_whatsapp_template_uses_qr_and_final_amount():
    payload = whatsapp_template_payload(
        recipient="919876543210",
        qr_url="https://cdn.example.com/qr.png",
        winner_name="Asha",
        product_name="Noir 10ml Partial",
        sale_name="Friday drop",
        amount=2400,
        template_name="payment_request",
    )
    assert payload["to"] == "919876543210"
    assert payload["template"]["components"][0]["parameters"][0]["image"]["link"] == "https://cdn.example.com/qr.png"
    assert payload["template"]["components"][1]["parameters"][2]["text"] == "₹2400"
