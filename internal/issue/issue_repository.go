package issue

import (
	"context"
	"errors"
	"time"
	"ydb-sample/internal/query"
	"ydb-sample/internal/utils"

	"github.com/google/uuid"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	ydbQuery "github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
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

func (repo *IssueRepository) AddIssues(issues []string) error {
	var queryParams = ydb.ParamsBuilder().
		Param("$args").
		BeginList().
		AddItems(
			utils.Mapped(&issues, func(i int, issue string) types.Value {
				return types.StructValue(
					types.StructFieldValue("id", types.UuidValue(uuid.New())),
					types.StructFieldValue("title", types.TextValue(issue)),
					types.StructFieldValue("created_at", types.TimestampValueFromTime(time.Now())),
				)
			})...,
		).
		EndList().
		Build()

	return repo.helper.ExecuteWithParams(`
		DECLARE $args AS List<Struct<
			id: Uuid,
			title: Text,
			created_at: Timestamp,
		>>;

		UPSERT INTO issues
		SELECT * FROM AS_TABLE($args);
		`,
		ydbQuery.SerializableReadWriteTxControl(ydbQuery.CommitTx()),
		queryParams,
	)
}

func (repo *IssueRepository) FindAll() ([]Issue, error) {
	var result = make([]Issue, 0)

	var err = repo.helper.Query(`
		SELECT
			id,
			title,
			created_at,
			author,
			COALESCE(links_count, 0) AS links_count,
			status
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

func (repo *IssueRepository) FindById(id uuid.UUID) (*Issue, error) {
	var result = make([]Issue, 0)

	var err = repo.helper.Query(`
		SELECT
			id,
			title,
			created_at,
			author,
			COALESCE(links_count, 0) AS links_count,
			status
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

func (repo *IssueRepository) FindByIds(ids []uuid.UUID) ([]Issue, error) {
	var result = make([]Issue, 0)

	var queryParams = ydb.ParamsBuilder().
		Param("$ids").
		BeginList().
		AddItems(
			utils.Mapped(&ids, func(i int, id uuid.UUID) types.Value {
				return types.StructValue(
					types.StructFieldValue("id", types.UuidValue(id)),
				)
			})...,
		).
		EndList().
		Build()

	var err = repo.helper.Query(`
		DECLARE $ids AS List<Struct<id: Uuid>>;

		SELECT
			id,
			title,
			created_at,
			author,
			links_count,
			status
		FROM issues
		WHERE id IN (SELECT id from AS_TABLE($ids));
		`,
		ydbQuery.SerializableReadWriteTxControl(ydbQuery.CommitTx()),
		queryParams,
		func(rs ydbQuery.ResultSet, ctx context.Context) error {
			return query.Materialize(rs, ctx, &result)
		},
	)
	if err != nil {
		return result, err
	}

	return result, nil
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
			COALESCE(links_count, 0) AS links_count,
			status
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

func (repo *IssueRepository) FindFutures() ([]IssueTitle, error) {
	var result = make([]IssueTitle, 0)

	var err = repo.helper.Query(`
		$future =
			SELECT id, title
			FROM issues
			WHERE status = 'FUTURE';
		
		SELECT * from $future;

		UPDATE issues ON
		SELECT
			id,
			CurrentUtcTimestamp() AS created_at,
			CAST('NEW' AS Text) AS status                                                
        FROM $future;
		`,
		ydbQuery.SerializableReadWriteTxControl(ydbQuery.CommitTx()),
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

func (repo *IssueRepository) UpdateStatus(id uuid.UUID, status string) error {
	return repo.helper.ExecuteWithParams(`
		DECLARE $id AS Uuid;
		DECLARE $new_status AS Text;

		UPDATE issues
		SET status = $new_status
		WHERE id = $id;
		`,
		ydbQuery.SerializableReadWriteTxControl(ydbQuery.CommitTx()),
		ydb.ParamsBuilder().
			Param("$id").Uuid(id).
			Param("$new_status").Text(status).
			Build(),
	)
}

func (repo *IssueRepository) Delete(id uuid.UUID) error {
	return repo.helper.ExecuteWithParams(`
		DECLARE $id AS Uuid;

		DELETE FROM issues WHERE id=$id
		`,
		ydbQuery.SerializableReadWriteTxControl(ydbQuery.CommitTx()),
		ydb.ParamsBuilder().
			Param("$id").Uuid(id).
			Build(),
	)
}

func (repo *IssueRepository) DeleteByIds(ids []uuid.UUID) error {
	var queryParams = ydb.ParamsBuilder().
		Param("$issues_ids_arg").
		BeginList().
		AddItems(
			utils.Mapped(&ids, func(i int, id uuid.UUID) types.Value {
				return types.UuidValue(id)
			})...,
		).
		EndList().
		Build()

	return repo.helper.ExecuteWithParams(`
			DECLARE $issues_ids_arg AS List<Uuid>;

			$list_to_id_struct = ($id) -> { RETURN <|id:$id|> };

			$issue_ids_list = ListMap(ListUniq($issues_ids_arg), $list_to_id_struct);

			$issues = SELECT id FROM AS_TABLE($issue_ids_list);

			$linked_issues = 
				SELECT
					source,
					destination
				FROM links
				WHERE source IN $issues;
			
			$linked_issues_mirrored =
				SELECT
					destination AS source,
					source AS destination
				FROM $linked_issues;
			
			$mirrored_dec_map = 
				SELECT
					source AS id,
					COUNT(*) as cnt
				FROM $linked_issues_mirrored
				GROUP BY source;
			
			UPDATE issues ON
			SELECT
				i.id as id,
				i.links_count - d.cnt AS links_count
			FROM $mirrored_dec_map AS d
			JOIN issues AS i ON d.id = i.id;

			UPDATE issues
			SET links_count = links_count - 1
			WHERE id IN $issues;

			DELETE FROM issues
			WHERE id IN $issues;
		`,
		ydbQuery.SerializableReadWriteTxControl(ydbQuery.CommitTx()),
		queryParams,
	)
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
