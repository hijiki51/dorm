package dorm

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
)

func testtestItemPutItem(t *testing.T) {
	t.Parallel()
	type args struct {
		ctx  context.Context
		db   *dynamodb.Client
		item testItem
		expr expression.Expression
	}
	tests := map[string]struct {
		args args

		wantErr    bool
		opts       []cmp.Option
		setup      func(*testing.T, *args)
		selfAssert []func(t *testing.T, args args)
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

				// randomize
				o := testItem{}
				err = RandomizeDDBStruct(&o)
				assert.NoError(t, err)

				// put item
				err = PutItem(args.ctx, args.db, o, expression.Expression{})
				assert.NoError(t, err)

				// set args

				args.item = o

				args.expr = expression.Expression{}
				assert.NoError(t, err)

			},
			selfAssert: []func(t *testing.T, args args){
				func(t *testing.T, args args) {
					got, err := GetItem[testItem](args.ctx, args.db, testItemPrimaryIndex{HashKey: args.item.HashKey}, expression.Expression{})
					assert.NoError(t, err)
					assert.NotNil(t, got)
					assert.Equal(t, args.item, *got)
				},
			},
			opts: []cmp.Option{
				cmpopts.IgnoreUnexported(testItem{}),
			},
		},
	}
	for name, tt := range tests {
		tt := tt
		tt.setup(t, &tt.args)
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := PutItem(tt.args.ctx, tt.args.db, tt.args.item, tt.args.expr); (err != nil) != tt.wantErr {
				t.Errorf("wantErr is %t, but err is %v", tt.wantErr, err)
			}
			for _, fn := range tt.selfAssert {
				fn(t, tt.args)
			}
		})
	}
}

func testtestItemBatchPutItem(t *testing.T) {
	t.Parallel()
	type args struct {
		ctx context.Context
		db  *dynamodb.Client

		items []testItem
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

				args.items = make([]testItem, len)

				for i := 0; i < len; i++ {
					// randomize
					o := testItem{}
					err = RandomizeDDBStruct(&o)
					assert.NoError(t, err)
					args.items[i] = o
				}

			},
			selfAssert: []func(t *testing.T, args *args){
				func(t *testing.T, args *args) {
					indices := []PrimaryIndex{}
					for _, item := range args.items {
						indices = append(indices, testItemPrimaryIndex{HashKey: item.HashKey})
					}
					db, err := ddbMain.conn()
					assert.NoError(t, err)
					proj := ProjectionAll[testItem]()
					expr, err := expression.NewBuilder().WithProjection(proj).Build()
					assert.NoError(t, err)
					got, err := BatchGetItems[testItem](args.ctx, db, indices, expr)
					assert.NoError(t, err)
					assert.NotNil(t, got)
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
			if err := BatchPutItem(tt.args.ctx, tt.args.db, tt.args.items); (err != nil) != tt.wantErr {
				t.Errorf("wantErr is %t, but err is %v", tt.wantErr, err)
			}
			for _, fn := range tt.selfAssert {
				fn(t, &tt.args)
			}
		})
	}
}
