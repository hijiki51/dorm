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

func testtestItemUpdateItem(t *testing.T) {
	t.Parallel()
	type args struct {
		ctx  context.Context
		db   *dynamodb.Client
		idx  PrimaryIndex
		expr expression.Expression
	}
	tests := map[string]struct {
		args args
		want *testItem

		wantErr    bool
		opts       []cmp.Option
		setup      func(*testing.T, *args) *testItem
		selfAssert []func(t *testing.T, args args, want *testItem)
	}{
		"success": {
			args: args{
				ctx: context.Background(),
			},
			setup: func(t *testing.T, args *args) (want *testItem) {
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
				// re randomize
				hashkey := o.HashKey

				err = RandomizeDDBStruct(&o)
				assert.NoError(t, err)
				o.HashKey = hashkey
				args.idx = testItemPrimaryIndex{HashKey: hashkey}
				want = &o

				keyskipper := func(name string) bool {
					return name == testItemColumns.HashKey
				}

				upd := AttributeSetAll(o, keyskipper)

				args.expr, err = expression.NewBuilder().WithUpdate(upd).Build()
				assert.NoError(t, err)
				return want
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
			tt.want = tt.setup(t, &tt.args)
			got, err := UpdateItem[testItem](tt.args.ctx, tt.args.db, tt.args.idx, tt.args.expr)
			if (err != nil) != tt.wantErr {
				t.Errorf("wantErr is %t, but err is %v", tt.wantErr, err)
			}
			if diff := cmp.Diff(tt.want, got, tt.opts...); len(diff) > 0 {
				t.Errorf("Compare value is mismatch (-want +got):%s\n", diff)
			}
			for _, fn := range tt.selfAssert {
				fn(t, tt.args, tt.want)
			}
		})
	}
}
