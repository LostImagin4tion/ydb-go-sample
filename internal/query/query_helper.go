package query

import (
	"context"
	"errors"
	"io"
	"log"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

type QueryHelper struct {
	driver     *ydb.Driver
	ctx        context.Context
	cancelFunc context.CancelFunc
}

func NewQueryHelper(dsn string) *QueryHelper {
	ctx, cancel := context.WithCancel(context.Background())

	db, err := ydb.Open(ctx, dsn)
	if err != nil {
		log.Fatal(err)
	}

	return &QueryHelper{
		driver:     db,
		ctx:        ctx,
		cancelFunc: cancel,
	}
}

func (q *QueryHelper) Execute(yql string) error {
	return q.ExecuteTx(
		yql,
		query.NoTx(),
		ydb.ParamsBuilder().Build(),
	)
}

func (q *QueryHelper) ExecuteTx(
	yql string,
	txControl *query.TransactionControl,
	params ydb.Params,
) error {
	return q.driver.Query().Do(
		q.ctx,
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

func (q *QueryHelper) Query(
	yql string,
	txControl *query.TransactionControl,
	params ydb.Params,
	materializeFunc func(query.ResultSet, context.Context) error,
) error {
	return q.driver.Query().Do(
		q.ctx,
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

				materializeFunc(resultSet, q.ctx)
			}

			return nil
		},
	)
}

func (q *QueryHelper) Close() {
	q.driver.Close(q.ctx)
	q.cancelFunc()
}
