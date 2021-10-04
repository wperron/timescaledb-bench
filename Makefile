start:
	docker-compose up -d

hydrate:
	psql -h localhost -p 5432 -U postgres < cpu_usage.sql
	psql -h localhost -p 5432 -U postgres -d homework -c "\COPY cpu_usage FROM cpu_usage.csv CSV HEADER"

run:
	go run . -host=localhost:5432 -user=postgres -password=password -db=homework -file=query_params.csv -workers=5