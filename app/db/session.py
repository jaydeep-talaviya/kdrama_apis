from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

POSTGRES_DATABASE_URL = "postgresql+asyncpg://user:password@localhost/dbname"
engine = create_async_engine(POSTGRES_DATABASE_URL, echo=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine, class_=AsyncSession)

async def get_postgres_db():
    async with SessionLocal() as session:
        yield session