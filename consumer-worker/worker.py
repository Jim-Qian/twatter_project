import asyncio  # Runs the background Kafka consumption loop concurrently
import json
import os
from contextlib import asynccontextmanager  # Used to hook into FastAPI’s lifecycle (start/stop)
from typing import List

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from confluent_kafka import Consumer, KafkaException
from sqlalchemy import Column, Integer, String, Text, create_engine  # For defining and managing SQLite DB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_NAME = os.getenv("TWEETS", "tweets")
DB_PATH = os.getenv("DB_PATH", "/data/twatter.sqlite")

# The declarative base class for ORM models
Base = declarative_base()
# Adding index for author and ts to speed up search
class Tweet(Base):
    __tablename__ = "tweets"
    id = Column(String(64), primary_key=True)
    author = Column(String(40), index=True)
    text = Column(Text)
    ts = Column(Integer, index=True)  # Timestamp
    in_reply_to = Column(String(64), nullable=True)

# Creates the SQLite engine pointing at your volume.
# Auto-creates the table if it doesn’t exist.
# SessionLocal() gives a short-lived DB session (used inside with blocks).
engine = create_engine(f"sqlite:///{DB_PATH}")
Base.metadata.create_all(engine)
SessionLocal = sessionmaker(bind=engine)

consumer_conf = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "twatter-consumer",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

kconsumer = Consumer(consumer_conf)
kconsumer.subscribe([TOPIC_NAME])

async def consume_loop():
    while True:
        try:
            # Runs forever, polling Kafka every second. If no message → waits briefly (avoids blocking other async tasks).
            msg = kconsumer.poll(1.0)
            if msg is None:
                await asyncio.sleep(0)
                continue
            if msg.error():
                raise KafkaException(msg.error())

            # Adds a new row to table if it doesn't already exist
            payload = json.loads(msg.value())
            with SessionLocal() as session:
                if not session.get(Tweet, payload["id"]):
                    t = Tweet(
                        id=payload["id"],
                        author=payload["author"],
                        text=payload["text"],
                        ts=payload["ts"],
                        in_reply_to=payload.get("in_reply_to"),
                    )
                    session.add(t)
                    session.commit()

            kconsumer.commit(msg)
        except Exception:
            await asyncio.sleep(0.5)

# Runs your consume_loop() as a background task when the FastAPI app starts.
# When the app shuts down, it cancels the loop gracefully and closes the Kafka consumer.
@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(consume_loop())
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except Exception:
            pass
        kconsumer.close()

class TweetOut(BaseModel):
    id: str
    author: str
    text: str
    ts: int
    in_reply_to: str | None = None

# Fast API endpoints
app = FastAPI(title="twatter-consumer", lifespan=lifespan)

@app.get("/feed", response_model=List[TweetOut])
def feed(limit: int = Query(20, ge=1, le=200), author: str | None = None):
    try:
        with SessionLocal() as session:
            q = session.query(Tweet)
            if author:
                q = q.filter(Tweet.author == author)
            q = q.order_by(Tweet.ts.desc()).limit(limit)
            rows = q.all()
            return [
                TweetOut(
                    id=r.id,
                    author=r.author,
                    text=r.text,
                    ts=r.ts,
                    in_reply_to=r.in_reply_to,
                )
                for r in rows
            ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/healthz")
def health():
    return {"status": "ok"}
