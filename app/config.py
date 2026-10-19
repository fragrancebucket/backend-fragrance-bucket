from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    mongodb_uri: str = "mongodb://localhost:27017"
    mongodb_database: str = "fragrance_bucket"
    jwt_secret: str = "development-only-change-me"
    web_origin: str = "http://localhost:3000"
    google_exchange_secret: str = "development-only-change-me"
    admin_email: str = ""
    imagekit_public_key: str = ""
    imagekit_private_key: str = ""
    imagekit_url_endpoint: str = ""
    whatsapp_access_token: str = ""
    whatsapp_phone_number_id: str = ""
    whatsapp_template_name: str = ""
    whatsapp_graph_api_version: str = "v23.0"
    whatsapp_verify_token: str = ""
    whatsapp_app_secret: str = ""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
