package query

import (
	"context"
	"errors"
	"io"
	"log"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic"
)

type QueryHelper struct {
	driver *ydb.Driver
	ctx    context.Context
}

func NewQueryHelper(ctx context.Context, dsn string) *QueryHelper {
	db, err := ydb.Open(ctx, dsn)
	if err != nil {
		log.Fatal(err)
	}

	return &QueryHelper{
		driver: db,
		ctx:    ctx,
	}
}

func (helper *QueryHelper) Execute(yql string) error {
	return helper.ExecuteWithParams(
		yql,
		query.NoTx(),
		ydb.ParamsBuilder().Build(),
	)
}

func (helper *QueryHelper) ExecuteWithParams(
	yql string,
	txControl *query.TransactionControl,
	params ydb.Params,
) error {
	return helper.driver.Query().Do(
		helper.ctx,
		func(ctx context.Context, s query.Session) error {
			err := s.Exec(
				ctx,
				yql,
				query.WithTxControl(txControl),
				query.WithParameters(params),
			)
			return err
		},
	)
}

func (helper *QueryHelper) ExecuteInTx(
	execute func(context.Context, query.TxActor) error,
) error {
	return helper.driver.Query().DoTx(
		helper.ctx,
		func(ctx context.Context, tx query.TxActor) error {
			return execute(ctx, tx)
		},
	)
}

func (helper *QueryHelper) Query(
	yql string,
	txControl *query.TransactionControl,
	params ydb.Params,
	materializeResult func(query.ResultSet, context.Context) error,
) error {
	return helper.driver.Query().Do(
		helper.ctx,
		func(ctx context.Context, s query.Session) error {
			result, err := s.Query(
				ctx,
				yql,
				query.WithTxControl(txControl),
				query.WithParameters(params),
			)

			if err != nil {
				return err
			}

			defer func() { _ = result.Close(ctx) }()

			for {
				resultSet, err := result.NextResultSet(ctx)
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}

					return err
				}

				err = materializeResult(resultSet, helper.ctx)
				if err != nil {
					return err
				}
			}

			return nil
		},
	)
}

func (helper *QueryHelper) Close() {
	helper.driver.Close(helper.ctx)
}

func (helper *QueryHelper) Topic() topic.Client {
	return helper.driver.Topic()
}

func (helper *QueryHelper) BulkUpsert(
	table string,
	data table.BulkUpsertData,
) error {
	return helper.driver.Table().BulkUpsert(
		helper.ctx,
		table,
		data,
	)
}

func (helper *QueryHelper) ReadTable(
	tableName string,
	materializeResult func(result.StreamResult, context.Context) error,
	opts ...options.ReadTableOption,
) error {
	return helper.driver.Table().Do(
		helper.ctx,
		func(ctx context.Context, s table.Session) error {
			result, err := s.StreamReadTable(ctx, tableName, opts...)
			if err != nil {
				return err
			}

			defer func() { _ = result.Close() }()

			return materializeResult(result, ctx)
		},
	)
}

func (helper *QueryHelper) ReadRows(
	tableName string,
	keys types.Value,
	readRowOpts ...options.ReadRowsOption,
) (result.Result, error) {
	return helper.driver.Table().ReadRows(
		helper.ctx,
		tableName,
		keys,
		readRowOpts,
	)
}
