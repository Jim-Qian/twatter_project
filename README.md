# Twatter

Two-service Kafka demo for posting and viewing tweets.

app.py:  
  •  /tweet: Send messages to the KAFKA topic "tweets"  
worker.py:  
  •  consume_loop:  Run forever and puts messages from topic "tweets" into sqlite's table "tweets".  
  •  /feed([limit], [author]): Return limit amount of tweets ordered by Tweets' ts: from newest to oldest.  

In docker-compose.yml, in "consumer-worker", the "volume" value of "./db:/data" means mapping "./db" in host machine to "/data" in container.

## Quick start
```
1) docker compose down           # Remove containers but not images.
2) docker compose pull           # Download all the images defined in docker-compose.yml. But does not start their respective containers.
3) docker compose up -d --build  # Start all containers.
4) docker compose ps             # List all the ports running
```

1) Post a tweet:
```bash
curl -X POST http://localhost:8000/tweet \\
  -H 'Content-Type: application/json' \\
  -d '{"author":"Jim","text":"1st Tweet!"}'
```
2) Read the feed:
```bash
curl 'http://localhost:8001/feed?limit=20'
```
