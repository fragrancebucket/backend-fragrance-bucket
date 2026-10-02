import jwt
from app.auth import issue_token
from app.config import settings


def test_issued_token_has_eight_hour_lifetime():
    token = issue_token("user", "admin")
    claims = jwt.decode(token, settings.jwt_secret, algorithms=["HS256"])
    assert 28_700 <= claims["exp"] - claims["iat"] <= 28_900
