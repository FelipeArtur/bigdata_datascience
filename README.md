# PostgreSQL, MongoDB e Spark: análise de compras em partidas

Projeto acadêmico de **Banco de Dados e Big Data para Data Science**, com duas entregas: migração relacional/documental (UA 1 e 2) e processamento Spark/Delta Lake (UA 3 e 4).

**Equipe:** Diogo Galrão Carvalho, Felipe Artur Macedo Lima e Luan Cavalcante Dias Rodrigues.

O fluxo segue **PostgreSQL → MongoDB → Spark + CSV demográfico → Delta Lake**. O pipeline compara cada campo das compras com o PostgreSQL e confere a tabela Delta após a gravação, buscando diferenças nas duas direções. Notebooks, relatórios e evidências registram a execução local.

> **Escopo acadêmico:** os roteiros citam MongoDB Atlas e Databricks. Este projeto executa tudo em Docker local, por decisão da equipe. A adaptação precisa ser aceita pelo docente. Não houve execução em nuvem.

## Executar tudo

Pré-requisito: Docker Engine com Compose v2. A construção inicial baixa imagens, pacotes Python e JARs; reserve alguns GB de disco e memória para os três serviços.

```bash
git clone https://github.com/FelipeArtur/bigdata_datascience.git
cd bigdata_datascience
make run
make test
```

Sem `make`, os comandos equivalentes são:

```bash
docker compose up -d --build --wait
docker compose exec -T jupyter python -m scripts.run_notebooks
docker compose exec -T jupyter python -m scripts.build_reports
docker compose exec -T jupyter python -m unittest discover -s tests -v
```

`make run` executa os três notebooks em ordem, preserva suas saídas, mede o benchmark, grava/reabre Delta e atualiza os dois PDFs. Falhas interrompem o fluxo. A migração substitui as quatro coleções **do banco de demonstração `loja_lol`**, após validar os dados temporários; não aponte esse comando para uma base de produção.

Jupyter: <http://localhost:8888>. Consulte o link com token em `docker compose logs jupyter`. Os bancos não publicam portas no host; ficam na rede interna do Compose. O ambiente Python roda como usuário UID 1000. Em Linux com outro UID, ajuste o usuário/permissões do bind mount antes de executar.

```bash
# Terminais dos bancos
docker compose exec postgres psql -U postgres -d loja_lol
docker compose exec mongodb mongosh loja_lol

# Parar, preservando volumes e arquivos
make down
```

PostgreSQL recebe os CSVs somente na inicialização de um volume vazio. Se a amostra for alterada, recarregue os dados no banco; a reconciliação do notebook rejeita divergências. Não é preciso excluir volumes para repetir esta amostra determinística.

## Entregas e evidências

- [Entrega 1: migração PostgreSQL/MongoDB](reports/Entrega_1_UA1_UA2_Relatorio_Tecnico.pdf)
- [Entrega 2: pipeline Spark/Delta](reports/Entrega_2_UA3_UA4_Relatorio_Tecnico.pdf)
- [Notebook 01: preparação](notebooks/01_exploracao_limpeza.ipynb)
- [Notebook 02: migração real](notebooks/02_migracao_mongodb.ipynb)
- [Notebook 03: processamento e benchmark](notebooks/03_pipeline_spark.ipynb)
- [Migração: contagens, BSON e comparação completa](reports/evidence/migration.json)
- [Pipeline: métricas, ambiente e tempos brutos](reports/evidence/pipeline.json)
- Planos físicos: [sort-merge](reports/evidence/plan_sort_merge.txt), [broadcast](reports/evidence/plan_broadcast.txt), [cache](reports/evidence/plan_cached.txt).

Cada integrante entrega individualmente um PDF por atividade no AVA. Edite o texto dos relatórios nos arquivos `.md`; `build_reports.py` atualiza apenas blocos de resultados delimitados e gera PDF com Markdown-it/WeasyPrint. HTML intermediário não é versionado. Para recompilar após editar o texto: `make reports`.

## Dados e resultados

Sete tabelas: 10 elos, 32 categorias, 254 itens, 834 associações item/categoria, 25 jogadores, 30 partidas e **1.043 compras**. A dimensão demográfica possui 25 registros.

O CSV bruto registra Data Dragon **16.19.1**, coletado em **25/09/2026, 01:42:44.216 UTC**, com URL de origem. Compras, partidas e demografia são **sintéticas**, reproduzíveis com seed 42. Seis itens de preço zero recebem 150 ouro na simulação. Nicks conhecidos são ilustrativos; não representam observações sobre as pessoas citadas.

Resultados da amostra:

- **2.228.821 ouro** movimentados; ouro não é receita monetária.
- **30,89%** do ouro em sábados e domingos.
- Ticket KR/Desafiante: **2.068,31** ouro por transação.
- Ticket KR/Grão-Mestre: **2.344,30**; maior grupo: BR/Grão-Mestre, **2.516,13**.
- VIP + Pro: **77,23%** do ouro no período de 20/08 a 20/09/2025.

O benchmark guarda cinco amostras por variante e medianas reais. Compara a mesma agregação, verifica igualdade dos resultados, controla AQE/broadcast automático e separa materialização do cache de reutilização. Os ganhos variam entre execuções. `local[2]` e esta base pequena não demonstram desempenho em um cluster distribuído.

## Estrutura mínima

```text
data/raw/          catálogo de entrada e sua proveniência
data/processed/    sete tabelas e CSV demográfico
notebooks/         narrativa e execução dos módulos compartilhados
scripts/           geração, migração, Spark, execução e relatórios
docker/init-db/    DDL e carga PostgreSQL
reports/           duas fontes Markdown, dois PDFs e evidências
tests/             regressões de integridade, Spark/Delta e renderização
```

Os documentos MongoDB são gerados diretamente na carga. JSON offline é opcional, com `python -m scripts.migrate_to_mongodb --source csv --export-only`, e fica ignorado pelo Git. O diretório Delta também é gerado e ignorado. As quatro coleções são `jogadores`, `itens`, `partidas` e `dominios`; a última preserva elos/categorias não utilizados.

O carregamento usa staging e rename por coleção; não é uma transação entre coleções. Pressupõe ausência de leitores concorrentes. Compras embutidas têm crescimento limitado pelos 16 MiB do BSON; não foi implementado sharding.

## Ambiente Python sem Docker

O caminho recomendado e validado é Docker. Para usar venv, forneça Python 3.12, Java 17, bibliotecas Pango/fontes do WeasyPrint e serviços PostgreSQL/MongoDB acessíveis. O DDL/carga precisa ser executado nesses serviços; venv não cria bancos.

```bash
make venv
source .venv/bin/activate
cp .env.example .env
# Ajuste .env para seus serviços locais.
make notebooks
python -m scripts.build_reports
```

As dependências diretas estão em `requirements.txt`. Spark 3.5.3, Delta 3.2.0 e MongoDB Spark Connector 10.4.0/Scala 2.12 formam a combinação testada. As versões transitivas podem variar em uma construção futura; por isso, builds futuros podem não ser idênticos bit a bit.
