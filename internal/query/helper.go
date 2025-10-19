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
	helper.cancelFunc()
}
