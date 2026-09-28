.PHONY: help venv data migrate reports docker-up docker-down docker-build docker-logs docker-migrate docker-psql docker-mongosh clean

help:
	@echo "======================================================================"
	@echo "COMANDOS DISPONÍVEIS - LOJA LEAGUE OF LEGENDS (BIG DATA PIPELINE)"
	@echo "======================================================================"
	@echo "Ambiente Local (venv):"
	@echo "  make venv            Configura o ambiente virtual Python (.venv) e instala dependências"
	@echo "  make data            Regenera todos os arquivos CSV em data/processed/"
	@echo "  make migrate         Executa o script de estruturação NoSQL e exporta JSONs"
	@echo "  make reports         Compila os relatórios técnicos oficiais em PDF (ABNT)"
	@echo ""
	@echo "Ambiente em Containers (Docker / Zero-Install):"
	@echo "  make docker-up       Inicia todos os serviços (PostgreSQL, MongoDB, Mongo-Express, Jupyter)"
	@echo "  make docker-down     Para e remove todos os containers"
	@echo "  make docker-build    Reconstrói a imagem Docker do ambiente JupyterLab"
	@echo "  make docker-logs     Acompanha os logs em tempo real dos serviços Docker"
	@echo "  make docker-migrate  Executa a migração NoSQL dentro do container do MongoDB"
	@echo "  make docker-psql     Abre o terminal interativo psql dentro do PostgreSQL"
	@echo "  make docker-mongosh  Abre o terminal interativo mongosh dentro do MongoDB"
	@echo "  make clean           Limpa arquivos temporários, caches e checkpoints"
	@echo "======================================================================"

venv:
	@bash scripts/setup_venv.sh

data:
	@python3 scripts/generate_harmonized_data.py

migrate:
	@python3 scripts/migrate_to_mongodb.py

reports:
	@python3 scripts/generate_report_ua1_ua2.py
	@python3 scripts/generate_report_ua3_ua4.py

docker-up:
	docker compose up -d
	@echo ""
	@echo "Serviços iniciados com sucesso!"
	@echo "   - JupyterLab:    http://localhost:8888"
	@echo "   - Mongo Express: http://localhost:8081"
	@echo "   - PostgreSQL:    localhost:5432 (banco: loja_lol, user: postgres)"
	@echo "   - MongoDB:       localhost:27017 (banco: loja_lol)"

docker-down:
	docker compose down

docker-build:
	docker compose build

docker-logs:
	docker compose logs -f

docker-migrate:
	docker compose exec jupyter python scripts/migrate_to_mongodb.py

docker-psql:
	docker compose exec postgres psql -U postgres -d loja_lol

docker-mongosh:
	docker compose exec mongodb mongosh loja_lol

clean:
	rm -rf __pycache__ */__pycache__ .ipynb_checkpoints */.ipynb_checkpoints *.tmp
