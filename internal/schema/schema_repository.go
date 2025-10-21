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
	if err != nil {
		log.Fatal(err)
	}

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

	err = repo.query.Execute(`
		CREATE TOPIC IF NOT EXISTS task_status(
			CONSUMER email
		) WITH(
			auto_partitioning_strategy = 'scale_up',
			min_active_partitions = 2,
			max_active_partitions = 10,
			retention_period = INTERVAL('P3D')
		);

		ALTER TABLE issues ADD COLUMN status Text;
	`)
	if err != nil {
		log.Fatal(err)
	}

	err = repo.query.Execute(`
		ALTER TABLE issues ADD CHANGEFEED updates WITH (
			FORMAT = 'JSON',
			MODE = 'NEW_AND_OLD_IMAGES',
			VIRTUAL_TIMESTAMPS = TRUE,
			INITIAL_SCAN = TRUE
		);
	`)
	if err != nil {
		log.Fatal(err)
	}

	err = repo.query.Execute("ALTER TOPIC `issues/updates` ADD CONSUMER test;")
	if err != nil {
		log.Fatal(err)
	}
}

func (repo *SchemaRepository) DropSchema() {
	err := repo.query.Execute(`
		DROP TABLE IF EXISTS issues;
		DROP TABLE IF EXISTS links;
		DROP TOPIC IF EXISTS task_status;
	`)
	if err != nil {
		log.Fatal(err)
	}
}
