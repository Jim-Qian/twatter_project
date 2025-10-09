# import asyncio  # Runs the background Kafka consumption loop concurrently
import json
import os
from contextlib import asynccontextmanager  # Used to hook into FastAPI’s lifecycle (start/stop)
import threading
from time import sleep
from typing import List

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from confluent_kafka import Consumer, KafkaException
from sqlalchemy import Column, Integer, String, Text, create_engine, event  # For defining and managing SQLite DB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool


BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_NAME = os.getenv("TWEETS", "tweets")
DB_PATH = os.getenv("DB_PATH", "/data/twatter.sqlite")

stop_event = threading.Event()

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
engine = create_engine(
    f"sqlite:///{DB_PATH}",
    connect_args={"check_same_thread": False, "timeout": 1.0},
    poolclass=NullPool,          # No pooling, new connection each time
)

Base.metadata.create_all(engine)
SessionLocal = sessionmaker(bind=engine)

@event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_conn, _):
    cur = dbapi_conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL")     # readers don’t block writers
    cur.execute("PRAGMA synchronous=NORMAL")   # faster fsyncs (fine for dev)
    cur.execute("PRAGMA busy_timeout=1000")    # 1s instead of 5s
    cur.close()


def consume_loop():
    while not stop_event.is_set():  # Python for thread, only has .start() no .end()
        try:
            # Polls from Kafka. If nothing returned, block for 1 second.
            msg = kconsumer.poll(1.0)
            if msg is None:
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
            sleep(0.5)

class TweetOut(BaseModel):
    id: str
    author: str
    text: str
    ts: int
    in_reply_to: str | None = None


# Runs your consume_loop() as a background task when the FastAPI app starts.
# When the app shuts down, it cancels the loop gracefully and closes the Kafka consumer.
@asynccontextmanager
async def lifespan(app: FastAPI):
    global kconsumer
    # create consumer here (not at import time)
    kconsumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "twatter-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    kconsumer.subscribe([TOPIC_NAME])

    # start background thread
    t = threading.Thread(target=consume_loop, daemon=True)
    t.start()
    try:
        yield
    finally:  # Runs when app shuts down. Stop thread and close consumer.
        stop_event.set()
        try:
            kconsumer.close()
        except Exception:
            pass
        t.join(timeout=2.0)

# Fast API endpoints
# Runs everything before yield in lifespan once at startup. Runs everything after once at shut down.
app = FastAPI(title="twatter-consumer", lifespan=lifespan)

# limit's default is 20; [1, 200] is the range.
# It returns limit amount of tweets ordered by Tweets' ts: from newest to oldest.
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

