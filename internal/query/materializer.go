package query

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
)

func Materialize[T any](
	resultSet query.ResultSet,
	ctx context.Context,
	target *[]T,
) error {
	for row, err := range sugar.UnmarshalRows[T](resultSet.Rows(ctx)) {
		if err != nil {
			clear(*target)
			return err
		}
		*target = append(*target, row)
	}
	return nil
}
