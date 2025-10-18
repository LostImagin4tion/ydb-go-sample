package issue

import (
	"context"
	"errors"
	"time"
	"ydb-sample/internal/query"

	"github.com/google/uuid"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	ydbQuery "github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
)

type IssueRepository struct {
	query *query.QueryHelper
}

func NewIssueRepository(query *query.QueryHelper) *IssueRepository {
	return &IssueRepository{
		query: query,
	}
}

func (repo *IssueRepository) AddIssue(title string) (*Issue, error) {
	var uuid = uuid.New()
	var timestamp = time.Now()

	var err = repo.query.ExecuteTx(`
		DECLARE $id AS Uuid;
		DECLARE $title AS Text;
		DECLARE $created_at AS Timestamp;
		
		UPSERT INTO issues (id, title, created_at)
		VALUES ($id, $title, $created_at);
		`,
		ydbQuery.SerializableReadWriteTxControl(ydbQuery.CommitTx()),
		ydb.ParamsBuilder().
			Param("$id").Uuid(uuid).
			Param("$title").Text(title).
			Param("$created_at").Timestamp(timestamp).
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

	repo.query.Query(`
		SELECT
			id,
			title,
			created_at
		FROM issues
		WHERE id=$id;
		`,
		ydbQuery.SnapshotReadOnlyTxControl(),
		ydb.ParamsBuilder().
			Param("$id").Uuid(id).
			Build(),
		func(resultSet ydbQuery.ResultSet, ctx context.Context) error {
			for row, err := range sugar.UnmarshalRows[Issue](resultSet.Rows(ctx)) {
				if err != nil {
					clear(result)
					return err
				}

				result = append(result, row)
			}
			return nil
		},
	)

	if len(result) > 1 {
		return nil, errors.New("Multiple rows with the same id")
	}
	if len(result) == 0 {
		return nil, errors.New("Didnt found any issues (lol)")
	}
	return &result[0], nil
}

func (repo *IssueRepository) FindAll() ([]Issue, error) {
	var result = make([]Issue, 0)

	var err = repo.query.Query(`
		SELECT
			id,
			title,
			created_at
		FROM issues;
		`,
		ydbQuery.SnapshotReadOnlyTxControl(),
		ydb.ParamsBuilder().Build(),
		func(resultSet ydbQuery.ResultSet, ctx context.Context) error {
			for row, err := range sugar.UnmarshalRows[Issue](resultSet.Rows(ctx)) {
				if err != nil {
					clear(result)
					return err
				}

				result = append(result, row)
			}
			return nil
		},
	)

	if err != nil {
		return result, err
	}

	return result, nil
}
