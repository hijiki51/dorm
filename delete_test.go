package dorm

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
)

func testtestItemDeleteItem(t *testing.T) {
	t.Parallel()
	type args struct {
		ctx context.Context
		db  *dynamodb.Client
		idx PrimaryIndex

		expr expression.Expression
	}
	tests := map[string]struct {
		args args

		wantErr    bool
		opts       []cmp.Option
		setup      func(*testing.T, *args) string
		selfAssert []func(t *testing.T, args args, id string)
	}{
		"success": {
			args: args{
				ctx: context.Background(),
			},
			setup: func(t *testing.T, args *args) string {
				var err error

				// init db
				args.db, err = ddbMain.conn()
				assert.NoError(t, err)

				// randomize
				o := testItem{}
				err = RandomizeDDBStruct(&o)
				assert.NoError(t, err)

				// put item
				err = PutItem(args.ctx, args.db, o, expression.Expression{})
				assert.NoError(t, err)

				// set args

				args.idx = testItemPrimaryIndex{HashKey: o.HashKey}

				args.expr = expression.Expression{}
				assert.NoError(t, err)

				return o.HashKey

			},
			opts: []cmp.Option{
				cmpopts.IgnoreUnexported(testItem{}),
			},
			selfAssert: []func(t *testing.T, args args, id string){
				func(t *testing.T, args args, id string) {
					_, err := GetItem[testItem](args.ctx, args.db, testItemPrimaryIndex{HashKey: id}, expression.Expression{})
					assert.True(t, errors.Is(err, ErrItemNotFound))
				},
			},
		},
	}
	for name, tt := range tests {
		tt := tt
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			id := tt.setup(t, &tt.args)
			if err := DeleteItem[testItem](tt.args.ctx, tt.args.db, tt.args.idx, tt.args.expr); (err != nil) != tt.wantErr {
				t.Errorf("wantErr is %t, but err is %v", tt.wantErr, err)
			}
			for _, fn := range tt.selfAssert {
				fn(t, tt.args, id)
			}
		})
	}
}

func testtestItemBatchDeleteItem(t *testing.T) {
	t.Parallel()
	type args struct {
		ctx context.Context
		db  *dynamodb.Client

		keys []PrimaryIndex
	}
	tests := map[string]struct {
		args       args
		setup      func(*testing.T, *args)
		wantErr    bool
		opts       []cmp.Option
		selfAssert []func(t *testing.T, args *args)
	}{
		"success": {
			args: args{
				ctx: context.Background(),
			},
			setup: func(t *testing.T, args *args) {
				var err error

				// init db
				args.db, err = ddbMain.conn()
				assert.NoError(t, err)

				len := int(NewRandomNonZeroInt()%100 + 1)

				args.keys = make([]PrimaryIndex, len)
				items := make([]testItem, len)

				for i := 0; i < len; i++ {
					// randomize
					o := testItem{}
					err = RandomizeDDBStruct(&o)
					assert.NoError(t, err)
					args.keys[i] = testItemPrimaryIndex{HashKey: o.HashKey}
					items[i] = o
				}
				start := 0
				end := start + maxBatchDeleteSize
				for start < len {
					if end > len {
						end = len
					}

					writeReqs := make([]types.WriteRequest, end-start)
					for i, item := range items[start:end] {
						av, err := attributevalue.MarshalMap(item)
						assert.NoError(t, err)

						writeReqs[i] = types.WriteRequest{
							PutRequest: &types.PutRequest{
								Item: av,
							},
						}

						start = end
						end = start + maxBatchDeleteSize

					}
					_, err = args.db.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
						RequestItems: map[string][]types.WriteRequest{
							*getFullTableName[testItem](): writeReqs,
						},
					})

					assert.NoError(t, err)

				}

			},
			selfAssert: []func(t *testing.T, args *args){
				func(t *testing.T, args *args) {
					db, err := ddbMain.conn()
					assert.NoError(t, err)

					proj := ProjectionAll[testItem]()
					expr, err := expression.NewBuilder().WithProjection(proj).Build()
					assert.NoError(t, err)
					got, err := BatchGetItems[testItem](args.ctx, db, args.keys, expr)
					assert.NoError(t, err)
					assert.Len(t, got, 0)
				},
			},
			opts: []cmp.Option{
				cmpopts.IgnoreUnexported(testItem{}),
			},
		},
	}
	for name, tt := range tests {
		tt := tt
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			tt.setup(t, &tt.args)
			if err := BatchDeleteItem[testItem](tt.args.ctx, tt.args.db, tt.args.keys); (err != nil) != tt.wantErr {
				t.Errorf("wantErr is %t, but err is %v", tt.wantErr, err)
			}
			for _, fn := range tt.selfAssert {
				fn(t, &tt.args)
			}
		})
	}
}
