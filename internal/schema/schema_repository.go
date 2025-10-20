package schema

import (
	"log"
	"ydb-sample/internal/query"
)

type SchemaRepository struct {
	query *query.QueryHelper
}

func NewSchemaRepository(query *query.QueryHelper) *SchemaRepository {
	return &SchemaRepository{
		query: query,
	}
}

func (repo *SchemaRepository) CreateSchema() {
	err := repo.query.Execute(`
		CREATE TABLE IF NOT EXISTS issues (
			id Uuid NOT NULL,
			title Text NOT NULL,
			created_at Timestamp NOT NULL,
			author Text,
			PRIMARY KEY (id)
		);
	`)

	if err != nil {
		log.Fatal(err)
	}

	err = repo.query.Execute(`
		ALTER TABLE issues ADD INDEX authorIndex GLOBAL ON (author);
	`)

	err = repo.query.Execute(`
		ALTER TABLE issues ADD COLUMN links_count Uint64;

		CREATE TABLE IF NOT EXISTS links (
			source Uuid NOT NULL,
			destination Uuid NOT NULL,
			PRIMARY KEY (source, destination)
		);
	`)

	if err != nil {
		log.Fatal(err)
	}
}

func (repo *SchemaRepository) DropSchema() {
	err := repo.query.Execute(`
		DROP TABLE IF EXISTS issues;
		DROP TABLE IF EXISTS links;
	`)

	if err != nil {
		log.Fatal(err)
	}
}
