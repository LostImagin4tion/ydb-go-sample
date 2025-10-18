package main

import (
	"context"
	"log"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if (err != nil) {
		log.Fatal(err)
	}
	defer db.Close(ctx)

	err = db.Query().Do(
		ctx,
		func(ctx context.Context, s query.Session) error {
			streamResult, err := s.Query(ctx, `SELECT 1 as id;`)

			if err != nil {
				return err;
			}

			defer func() { _ = streamResult.Close(ctx) }()

			for rs, err := range streamResult.ResultSets(ctx) {
				if err != nil {
					return err
				}

				for row, err := range rs.Rows(ctx) {
					if err != nil {
						return err
					}

					type myStruct struct {
						Id int32 `sql:"id"`
					}
					var s myStruct;
					if err = row.ScanStruct(&s); err != nil {
						return err;
					}

					println(s.Id)
				}
			}
			return nil
		},
		query.WithIdempotent(),
	)

	if err != nil {
		log.Fatal(err)
	}
}
