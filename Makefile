
.PHONY: run
run:
	go run cmd/main.go

.PHONY: deploy
deploy:
	docker compose -f deployment/docker-compose.yml up --build