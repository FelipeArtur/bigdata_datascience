.PHONY: up down run test reports venv data migrate pipeline notebooks

up:
	docker compose up -d --build --wait

down:
	docker compose down

# Fluxo completo: os notebooks usam os mesmos módulos das CLIs.
run: up
	docker compose exec -T jupyter python -m scripts.run_notebooks
	docker compose exec -T jupyter python -m scripts.build_reports

test:
	docker compose exec -T jupyter python -m unittest discover -s tests -v

reports:
	docker compose exec -T jupyter python -m scripts.build_reports

venv:
	python3 -m venv .venv
	.venv/bin/pip install -r requirements.txt

data:
	python -m scripts.generate_harmonized_data

migrate:
	python -m scripts.migrate_to_mongodb --source postgres

pipeline:
	python -m scripts.spark_pipeline

notebooks:
	python -m scripts.run_notebooks
