package bulk

import (
	"context"
	"log"
	"time"
	"ydb-sample/internal/issue"
	"ydb-sample/internal/query"
	"ydb-sample/internal/utils"

	"github.com/google/uuid"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

type KeyValueApiRepository struct {
	query *query.QueryHelper
}

func NewKeyValueApiRepository(query *query.QueryHelper) *KeyValueApiRepository {
	return &KeyValueApiRepository{
		query: query,
	}
}

func (repo *KeyValueApiRepository) BulkUpsert(
	tableName string,
	titleAuthorList []issue.TitleAuthor,
) error {
	var values []types.Value = utils.Mapped(
		&titleAuthorList,
		func(i int, issue issue.TitleAuthor) types.Value {
			return types.StructValue(
				types.StructFieldValue("id", types.UuidValue(uuid.New())),
				types.StructFieldValue("title", types.TextValue(issue.Title)),
				types.StructFieldValue("author", types.TextValue(issue.Author)),
				types.StructFieldValue("created_at", types.TimestampValueFromTime(time.Now())),
			)
		},
	)

	return repo.query.BulkUpsert(
		tableName,
		table.BulkUpsertDataRows(types.ListValue(values...)),
	)
}

func (repo *KeyValueApiRepository) ReadTable(table string) ([]issue.Issue, error) {
	resultIssues := make([]issue.Issue, 0)

	err := repo.query.ReadTable(
		table,
		func(rs result.StreamResult, ctx context.Context) error {
			for rs.NextResultSet(ctx) {
				for rs.NextRow() {
					issue := issue.Issue{}

					err := rs.ScanNamed(
						named.Required("id", &issue.Id),
						named.Required("title", &issue.Title),
						named.Required("created_at", &issue.Timestamp),
					)
					if err != nil {
						return err
					}
					log.Println("pupup")

					resultIssues = append(resultIssues, issue)
				}
			}
			return rs.Err()
		},
		options.ReadOrdered(),
		options.ReadFromSnapshot(true),
		options.ReadColumns("id", "title", "created_at"),
	)
	if err != nil {
		return resultIssues, err
	}

	return resultIssues, nil
}

func (repo *KeyValueApiRepository) ReadRows(
	ctx context.Context,
	table string,
	id uuid.UUID,
) ([]issue.Issue, error) {
	resultIssues := make([]issue.Issue, 0)

	result, err := repo.query.ReadRows(
		table,
		types.ListValue(
			types.StructValue(
				types.StructFieldValue("id", types.UuidValue(id)),
			),
		),
		options.ReadColumns("id", "title", "created_at"),
	)
	if err != nil {
		return resultIssues, err
	}

	defer func() { _ = result.Close() }()

	for result.NextResultSet(ctx) {
		for result.NextRow() {
			issue := issue.Issue{}

			err := result.ScanNamed(
				named.Required("id", &issue.Id),
				named.Required("title", &issue.Title),
				named.Required("created_at", &issue.Timestamp),
			)
			if err != nil {
				return resultIssues, err
			}

			resultIssues = append(resultIssues, issue)
		}
	}
	if err = result.Err(); err != nil {
		return resultIssues, err
	}

	return resultIssues, nil
}
