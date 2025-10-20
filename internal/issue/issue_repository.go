package issue

import (
	"context"
	"errors"
	"time"
	"ydb-sample/internal/query"

	"github.com/google/uuid"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	ydbQuery "github.com/ydb-platform/ydb-go-sdk/v3/query"
)

type IssueRepository struct {
	helper *query.QueryHelper
}

func NewIssueRepository(helper *query.QueryHelper) *IssueRepository {
	return &IssueRepository{
		helper: helper,
	}
}

func (repo *IssueRepository) AddIssue(
	title string,
	author string,
) (*Issue, error) {
	var uuid = uuid.New()
	var timestamp = time.Now()

	var err = repo.helper.ExecuteWithParams(`
		DECLARE $id AS Uuid;
		DECLARE $title AS Text;
		DECLARE $created_at AS Timestamp;
		DECLARE $author as Text;
		
		UPSERT INTO issues (id, title, created_at, author)
		VALUES ($id, $title, $created_at, $author);
		`,
		ydbQuery.SerializableReadWriteTxControl(ydbQuery.CommitTx()),
		ydb.ParamsBuilder().
			Param("$id").Uuid(uuid).
			Param("$title").Text(title).
			Param("$created_at").Timestamp(timestamp).
			Param("$author").Text(author).
			Build(),
	)
	if err != nil {
		return nil, err
	}

	return &Issue{
		Id:        uuid,
		Title:     title,
		Timestamp: timestamp,
	}, nil
}

func (repo *IssueRepository) FindById(id uuid.UUID) (*Issue, error) {
	var result = make([]Issue, 0)

	var err = repo.helper.Query(`
		SELECT
			id,
			title,
			created_at,
			author,
			COALESCE(links_count, 0) AS links_count
		FROM issues
		WHERE id=$id;
		`,
		ydbQuery.SnapshotReadOnlyTxControl(),
		ydb.ParamsBuilder().
			Param("$id").Uuid(id).
			Build(),
		func(rs ydbQuery.ResultSet, ctx context.Context) error {
			return query.Materialize(rs, ctx, &result)
		},
	)
	if err != nil {
		return nil, err
	}

	if len(result) > 1 {
		return nil, errors.New("Multiple rows with the same id")
	}
	if len(result) == 0 {
		return nil, errors.New("Didnt found any issues (lol)")
	}

	return &result[0], nil
}

func (repo *IssueRepository) FindByAuthor(author string) ([]Issue, error) {
	var result = make([]Issue, 0)

	var err = repo.helper.Query(`
		DECLARE $author AS Text;

		SELECT
			id,
			title,
			created_at,
			author,
			COALESCE(links_count, 0) AS links_count
		FROM issues
		WHERE author=$author
		`,
		ydbQuery.SnapshotReadOnlyTxControl(),
		ydb.ParamsBuilder().
			Param("$author").Text(author).
			Build(),
		func(rs ydbQuery.ResultSet, ctx context.Context) error {
			return query.Materialize(rs, ctx, &result)
		},
	)
	if err != nil {
		return result, err
	}

	return result, nil
}

func (repo *IssueRepository) FindAll() ([]Issue, error) {
	var result = make([]Issue, 0)

	var err = repo.helper.Query(`
		SELECT
			id,
			title,
			created_at,
			author,
			COALESCE(links_count, 0) AS links_count
		FROM issues;
		`,
		ydbQuery.SnapshotReadOnlyTxControl(),
		ydb.ParamsBuilder().Build(),
		func(rs ydbQuery.ResultSet, ctx context.Context) error {
			return query.Materialize(rs, ctx, &result)
		},
	)
	if err != nil {
		return result, err
	}

	return result, nil
}

func (repo *IssueRepository) LinkTicketsNoInteractive(
	id1 uuid.UUID,
	id2 uuid.UUID,
) ([]IssueLinksCount, error) {
	var result = make([]IssueLinksCount, 0)

	var err = repo.helper.Query(`
		DECLARE $t1 as Uuid;
		DECLARE $t2 as Uuid;

		UPDATE issues
		SET links_count = COALESCE(links_count, 0) + 1
		WHERE id IN ($t1, $t2);

		INSERT INTO links (source, destination)
		VALUES ($t1, $t2), ($t2, $t1);

		SELECT id, links_count FROM issues
		WHERE id in ($t1, $t2);
		`,
		ydbQuery.SerializableReadWriteTxControl(ydbQuery.CommitTx()),
		ydb.ParamsBuilder().
			Param("$t1").Uuid(id1).
			Param("$t2").Uuid(id2).
			Build(),
		func(rs ydbQuery.ResultSet, ctx context.Context) error {
			return query.Materialize(rs, ctx, &result)
		},
	)
	if err != nil {
		return result, err
	}

	return result, nil
}

func (repo *IssueRepository) LinkTicketsInteractive(
	id1 uuid.UUID,
	id2 uuid.UUID,
) ([]IssueLinksCount, error) {
	var result = make([]IssueLinksCount, 0)

	var err = repo.helper.ExecuteInTx(
		func(ctx context.Context, tx ydbQuery.TxActor) error {
			var err = tx.Exec(
				ctx,
				`
				DECLARE $t1 AS Uuid;
				DECLARE $t2 AS Uuid;

				UPDATE issues
				SET links_count = COALESCE(links_count, 0) + 1
				WHERE id in ($t1, $t2);
				`,
				ydbQuery.WithParameters(
					ydb.ParamsBuilder().
						Param("$t1").Uuid(id1).
						Param("$t2").Uuid(id2).
						Build(),
				),
			)
			if err != nil {
				return err
			}

			err = tx.Exec(
				ctx,
				`
				DECLARE $t1 as Uuid;
				DECLARE $t2 as Uuid;

				INSERT INTO links (source, destination)
				VALUES ($t1, $t2), ($t2, $t1);
				`,
				ydbQuery.WithParameters(
					ydb.ParamsBuilder().
						Param("$t1").Uuid(id1).
						Param("$t2").Uuid(id2).
						Build(),
				),
			)
			if err != nil {
				return err
			}

			rows, err := tx.QueryResultSet(
				ctx,
				`
				DECLARE $t1 as Uuid;
				DECLARE $t2 as Uuid;

				SELECT id, links_count FROM issues
				WHERE id IN ($t1, $t2);
				`,
				ydbQuery.WithParameters(
					ydb.ParamsBuilder().
						Param("$t1").Uuid(id1).
						Param("$t2").Uuid(id2).
						Build(),
				),
			)
			if err != nil {
				return err
			}

			return query.Materialize(rows, ctx, &result)
		},
	)
	if err != nil {
		return result, err
	}

	return result, nil
}
