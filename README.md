# Twatter

Two-service Kafka demo.

## Quick start

1) `make up`
2) Post a tweet:
```bash
curl -X POST http://localhost:8000/tweet \\
  -H 'Content-Type: application/json' \\
  -d '{"author":"alice","text":"hello twatter!"}'
```
3) Read the feed:
```bash
curl 'http://localhost:8001/feed?limit=20'
```
