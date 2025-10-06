import json
import os
import time
import uuid  # Generate unique IDs
from typing import Optional  # Type hint for a field that may be None

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field  # Request validation
from confluent_kafka import Producer

# Connect to Kafka running in the container named kafka, on port 9092
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_NAME = os.getenv("TWEETS", "tweets")

# bootstrap.servers: where to find the broker.
# enable.idempotence=True: avoids duplicates on retries (same record won’t be written twice).
# linger.ms=5: wait up to 5ms to batch messages (boosts throughput).
# batch.num.messages=1000: allow larger batches.
conf = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "enable.idempotence": True,
    "linger.ms": 5,
    "batch.num.messages": 1000,
}
producer = Producer(conf)

# Pydantic model that validates incoming JSON
class TweetIn(BaseModel):
    author: str = Field(min_length=1, max_length=40)  # char length
    text: str = Field(min_length=1, max_length=280)
    in_reply_to: Optional[str] = None

# Fast API endpoints
app = FastAPI(title="twatter-producer")

@app.post("/tweet")
# The incoming JSON is passed to BaseModel's constructor (since TweetIn didn't override), creating the t object.
# If JSON fails to convert to TweetIn, automatically returns a 422 erro.
def post_tweet(t: TweetIn):
    tweet = {
        "id": str(uuid.uuid4()),
        "author": t.author,
        "text": t.text,
        "in_reply_to": t.in_reply_to,
        "ts": int(time.time() * 1000),
    }

    key = t.author.encode("utf-8")
    value = json.dumps(tweet).encode("utf-8")

    try:
        # Enqueue the record to the client’s buffer
        producer.produce(TOPIC_NAME, key=key, value=value)
        # 
        producer.flush(5)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"failed to publish: {e}")

    return {"ok": True, "tweet": tweet}

@app.get("/healthz")
def health():
    return {"status": "ok"}
