import jwt
from app.auth import issue_token
from app.config import settings


def test_issued_token_has_thirty_day_lifetime():
    token = issue_token("user", "admin")
    claims = jwt.decode(token, settings.jwt_secret, algorithms=["HS256"])
    assert 2_591_900 <= claims["exp"] - claims["iat"] <= 2_592_100
