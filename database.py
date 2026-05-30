"""
db/database.py — SQLAlchemy engine + session factory
"""
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, DeclarativeBase

from app.config import settings


engine = create_engine(
    settings.DATABASE_URL,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
    echo=False,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    pass


def init_db() -> None:
    """
    Create all tables from ORM models if they don't already exist.
    Called on app startup — replaces the old init.sql file-mount approach
    that broke on Windows. Safe to call multiple times (checkfirst=True).
    """
    # Import models so SQLAlchemy knows about them before create_all
    import app.models  # noqa: F401
    Base.metadata.create_all(bind=engine, checkfirst=True)
    _ensure_log_metrics_constraint()


def _ensure_log_metrics_constraint() -> None:
    """Repair older databases where log_metrics was created without the upsert constraint."""
    with engine.begin() as conn:
        exists = conn.execute(
            text(
                """
                SELECT 1
                FROM pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                WHERE t.relname = 'log_metrics'
                  AND c.conname = 'uq_log_metrics_service_level_bucket'
                """
            )
        ).scalar()

        if exists:
            return

        conn.execute(
            text(
                """
                DELETE FROM log_metrics a
                USING log_metrics b
                WHERE a.ctid < b.ctid
                  AND a.service_name = b.service_name
                  AND a.level = b.level
                  AND a.bucket = b.bucket
                """
            )
        )
        conn.execute(
            text(
                """
                ALTER TABLE log_metrics
                ADD CONSTRAINT uq_log_metrics_service_level_bucket
                UNIQUE (service_name, level, bucket)
                """
            )
        )


def get_db():
    """FastAPI dependency — yields a DB session and ensures cleanup."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def health_check() -> bool:
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
