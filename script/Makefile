.PHONY: build
.DEFAULT_GOAL := build

PYTESTFLAGS=""

build:
	cd "$(dirname "$0")"
	python script/generate_sql --destination ../target/sql --udf-dir ../udf --sql-dir ../sql
	pytest $(PYTESTFLAGS)
