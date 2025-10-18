
.PHONY: run
run:
	go run main.go

.PHONY: deploy
deploy:
	docker compose -f deployment/docker-compose.yml up --build