.PHONY: restart down logs ps

restart:
	docker compose down
	docker compose pull
	docker compose up -d --build

down:
	docker compose down -v

logs:
	docker compose logs -f --tail=200

ps:
	docker compose ps