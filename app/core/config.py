from pydantic import BaseSettings

class Settings(BaseSettings):
    database_url: str = "sqlite:///./test.db"
    secret_key: str = "supersecretkey"

    class Config:
        env_file = ".env"

settings = Settings()
