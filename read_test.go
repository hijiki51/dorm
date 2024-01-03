package dorm

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
)

func testtestItemGetItem(t *testing.T) {
	t.Parallel()
	type args struct {
		ctx  context.Context
		db   *dynamodb.Client
		idx  PrimaryIndex
		expr expression.Expression
	}
	tests := map[string]struct {
		args       args
		want       *testItem
		setup      func(t *testing.T, args *args) *testItem
		wantErr    bool
		opts       []cmp.Option
		selfAssert []func(t *testing.T, args args, want *testItem)
	}{
		"success": {
			args: args{
				ctx: context.Background(),
			},
			want: &testItem{},
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
				args.idx = testItemPrimaryIndex{
					HashKey: o.HashKey,
				}

				proj := ProjectionAll[testItem]()
				args.expr, err = expression.NewBuilder().WithProjection(proj).Build()
				assert.NoError(t, err)

				// set want
				want = &o

				return want
			},
			opts: []cmp.Option{
				cmpopts.IgnoreUnexported(testItem{}),
			},
		},
		"empty": {
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

				// set args
				args.idx = testItemPrimaryIndex{
					HashKey: o.HashKey,
				}

				proj := ProjectionAll[testItem]()
				args.expr, err = expression.NewBuilder().WithProjection(proj).Build()
				assert.NoError(t, err)
				return nil
			},
			wantErr: true,
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
			got, err := GetItem[testItem](tt.args.ctx, tt.args.db, tt.args.idx, tt.args.expr)
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

func testtestItemBatchGetItems(t *testing.T) {
	t.Parallel()

	type args struct {
		ctx  context.Context
		db   *dynamodb.Client
		idxs []PrimaryIndex

		expr expression.Expression
	}
	tests := map[string]struct {
		args args
		want []testItem

		wantErr    bool
		setup      func(t *testing.T, args *args) []testItem
		opts       []cmp.Option
		selfAssert []func(t *testing.T, args args, want []testItem)
	}{
		"success": {
			args: args{
				ctx: context.Background(),
			},
			setup: func(t *testing.T, args *args) (want []testItem) {
				var err error
				// init db
				args.db, err = ddbMain.conn()
				assert.NoError(t, err)

				len := 150
				indices := make([]PrimaryIndex, len)
				want = make([]testItem, len)

				for i := 0; i < len; i++ {
					// randomize
					o := testItem{}
					err = RandomizeDDBStruct(&o)
					assert.NoError(t, err)

					// put item
					err = PutItem(args.ctx, args.db, o, expression.Expression{})
					assert.NoError(t, err)

					// add indices
					indices[i] = testItemPrimaryIndex{
						HashKey: o.HashKey,
					}

					// add want
					want[i] = o
				}
				// set args

				args.idxs = indices

				proj := ProjectionAll[testItem]()
				args.expr, err = expression.NewBuilder().WithProjection(proj).Build()
				assert.NoError(t, err)
				return want
			},
			opts: []cmp.Option{
				cmpopts.SortSlices(func(x, y testItem) bool {
					return x.HashKey < y.HashKey
				}),
				cmpopts.IgnoreUnexported(testItem{}),
			},
		},
		"empty": {
			args: args{
				ctx: context.Background(),
			},
			setup: func(t *testing.T, args *args) (want []testItem) {
				var err error

				// init db
				args.db, err = ddbMain.conn()
				assert.NoError(t, err)

				// randomize
				o := testItem{}
				err = RandomizeDDBStruct(&o)
				assert.NoError(t, err)

				// set args
				args.idxs = []PrimaryIndex{
					testItemPrimaryIndex{HashKey: o.HashKey},
				}

				proj := ProjectionAll[testItem]()
				args.expr, err = expression.NewBuilder().WithProjection(proj).Build()
				assert.NoError(t, err)

				return []testItem{}
			},
			opts: []cmp.Option{
				cmpopts.SortSlices(func(x, y testItem) bool {
					return x.HashKey < y.HashKey
				}),
				cmpopts.IgnoreUnexported(testItem{}),
			},
			wantErr: false,
		},
	}
	for name, tt := range tests {
		tt := tt
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			tt.want = tt.setup(t, &tt.args)
			got, err := BatchGetItems[testItem](tt.args.ctx, tt.args.db, tt.args.idxs, tt.args.expr)
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

func testtestItemQuery(t *testing.T) {
	t.Parallel()
	type args struct {
		ctx  context.Context
		db   *dynamodb.Client
		expr expression.Expression
		opts []QueryOptionFunc
	}
	tests := map[string]struct {
		args       args
		want       []testItem
		want1      map[string]types.AttributeValue
		setup      func(t *testing.T, args *args) ([]testItem, map[string]types.AttributeValue)
		wantErr    bool
		opts       []cmp.Option
		selfAssert []func(t *testing.T, args args, got []testItem, got1 map[string]types.AttributeValue, want []testItem, want1 map[string]types.AttributeValue)
	}{
		"success": {
			args: args{
				ctx: context.Background(),
			},
			setup: func(t *testing.T, args *args) (want []testItem, want1 map[string]types.AttributeValue) {
				var err error
				// init db
				args.db, err = ddbMain.conn()
				assert.NoError(t, err)

				len := int(NewRandomNonZeroInt()%100 + 100)

				var id string
				assert.NoError(t, err)

				for i := 0; i < len; i++ {
					// randomize
					o := testItem{}
					err = RandomizeDDBStruct(&o)

					assert.NoError(t, err)

					if i == 0 {
						id = o.HashKey
						// add want
						want = append(want, o)
					}

					// put item
					err = PutItem(args.ctx, args.db, o, expression.Expression{})
					assert.NoError(t, err)

				}
				// set args

				proj := ProjectionAll[testItem]()
				keycond := expression.Key(testItemColumns.HashKey).Equal(expression.Value(id))
				args.expr, err = expression.NewBuilder().WithProjection(proj).WithKeyCondition(keycond).Build()
				assert.NoError(t, err)
				return want, nil
			},
			opts: []cmp.Option{
				cmpopts.SortSlices(func(x, y testItem) bool {
					return x.HashKey < y.HashKey
				}),
				cmpopts.IgnoreUnexported(testItem{}),
			},
		},
		"success with gsi": {
			args: args{
				ctx: context.Background(),
			},
			setup: func(t *testing.T, args *args) (want []testItem, want1 map[string]types.AttributeValue) {
				var err error
				// init db
				args.db, err = ddbMain.conn()
				assert.NoError(t, err)

				len := int(NewRandomNonZeroInt()%100 + 100)

				hashkey, err := NewRandomEngStr(28)
				assert.NoError(t, err)

				rangekey, err := NewRandomEngStr(28)
				assert.NoError(t, err)

				for i := 0; i < len; i++ {
					// randomize
					o := testItem{}
					err = RandomizeDDBStruct(&o)

					assert.NoError(t, err)

					// at least 1 item with filter

					if i%2 == 0 {
						o.GSIHashKey = hashkey
						if i%3 == 0 {
							o.GSIRangeKey = rangekey
							want = append(want, o)
						}
					}

					// put item
					err = PutItem(args.ctx, args.db, o, expression.Expression{})
					assert.NoError(t, err)

				}
				// set args

				args.opts = []QueryOptionFunc{
					func(opt *QueryOptions) {
						opt.IndexName = &testItemIndexName.GSI
					},
				}

				proj := ProjectionAll[testItem]()
				keycond := expression.Key(testItemColumns.GSIHashKey).Equal(expression.Value(hashkey)).And(expression.Key(testItemColumns.GSIRangeKey).Equal(expression.Value(rangekey)))
				args.expr, err = expression.NewBuilder().WithProjection(proj).WithKeyCondition(keycond).Build()
				assert.NoError(t, err)
				return want, nil
			},
			opts: []cmp.Option{
				cmpopts.SortSlices(func(x, y testItem) bool {
					return x.HashKey < y.HashKey
				}),

				cmpopts.IgnoreUnexported(testItem{}),
			},
		},
		"success with gsi and filter": {
			args: args{
				ctx: context.Background(),
			},
			setup: func(t *testing.T, args *args) (want []testItem, want1 map[string]types.AttributeValue) {
				var err error
				// init db
				args.db, err = ddbMain.conn()
				assert.NoError(t, err)

				len := int(NewRandomNonZeroInt()%100 + 100)

				filt, err := NewRandomEngStr(10)
				assert.NoError(t, err)

				hashkey, err := NewRandomEngStr(28)
				assert.NoError(t, err)

				rangekey, err := NewRandomEngStr(28)
				assert.NoError(t, err)

				for i := 0; i < len; i++ {
					// randomize
					o := testItem{}
					err = RandomizeDDBStruct(&o)

					assert.NoError(t, err)

					// at least 1 item with filter
					if i == 0 {
						o.FilterKey = filt
						o.GSIRangeKey = rangekey
					}

					if i%3 == 0 {
						o.FilterKey = filt
					}

					if i%2 == 0 {
						o.GSIHashKey = hashkey
						if i%3 == 0 && i%5 == 0 {
							o.GSIRangeKey = rangekey

							// add want
							want = append(want, o)

						}

					}

					// put item
					err = PutItem(args.ctx, args.db, o, expression.Expression{})
					assert.NoError(t, err)

				}
				// set args

				args.opts = []QueryOptionFunc{
					func(opt *QueryOptions) {
						opt.IndexName = &testItemIndexName.GSI
					},
				}

				proj := ProjectionAll[testItem]()
				keycond := expression.Key(testItemColumns.GSIHashKey).Equal(expression.Value(hashkey)).And(expression.Key(testItemColumns.GSIRangeKey).Equal(expression.Value(rangekey)))
				filter := expression.Name(testItemColumns.FilterKey).Equal(expression.Value(filt))
				args.expr, err = expression.NewBuilder().WithProjection(proj).WithKeyCondition(keycond).WithFilter(filter).Build()
				assert.NoError(t, err)
				return want, nil
			},
			opts: []cmp.Option{
				cmpopts.SortSlices(func(x, y testItem) bool {
					return x.HashKey < y.HashKey
				}),

				cmpopts.IgnoreUnexported(testItem{}),
			},
		},
		"empty": {
			args: args{
				ctx: context.Background(),
			},
			setup: func(t *testing.T, args *args) (want []testItem, want1 map[string]types.AttributeValue) {
				var err error

				// init db
				args.db, err = ddbMain.conn()
				assert.NoError(t, err)

				// randomize
				o := testItem{}
				err = RandomizeDDBStruct(&o)
				assert.NoError(t, err)
				hashKey, err := NewRandomEngStr(28)
				assert.NoError(t, err)

				// set args

				proj := ProjectionAll[testItem]()
				keycond := expression.Key(testItemColumns.HashKey).Equal(expression.Value(hashKey))
				args.expr, err = expression.NewBuilder().WithProjection(proj).WithKeyCondition(keycond).Build()
				assert.NoError(t, err)

				return []testItem{}, nil
			},
			opts: []cmp.Option{
				cmpopts.SortSlices(func(x, y testItem) bool {
					return x.HashKey < y.HashKey
				}),

				cmpopts.IgnoreUnexported(testItem{}),
			},
			wantErr: false,
		},
	}
	for name, tt := range tests {
		tt := tt
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			tt.want, tt.want1 = tt.setup(t, &tt.args)
			got, got1, err := Query[testItem](tt.args.ctx, tt.args.db, tt.args.expr, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("wantErr is %t, but err is %v", tt.wantErr, err)
			}
			if diff := cmp.Diff(tt.want, got, tt.opts...); len(diff) > 0 {
				t.Errorf("Compare value is mismatch (-want +got):%s\n", diff)
			}
			if diff := cmp.Diff(tt.want1, got1, tt.opts...); len(diff) > 0 {
				t.Errorf("Compare value is mismatch (-want +got):%s\n", diff)
			}
			for _, fn := range tt.selfAssert {
				fn(t, tt.args, got, got1, tt.want, tt.want1)
			}
		})
	}
}
func testtestItemQueryAll(t *testing.T) {
	t.Parallel()
	type args struct {
		ctx  context.Context
		db   *dynamodb.Client
		expr expression.Expression
		opts []QueryOptionFunc
	}
	tests := map[string]struct {
		args       args
		want       []testItem
		setup      func(t *testing.T, args *args) []testItem
		wantErr    bool
		opts       []cmp.Option
		selfAssert []func(t *testing.T, args args, want []testItem)
	}{
		"success": {
			args: args{
				ctx: context.Background(),
			},
			setup: func(t *testing.T, args *args) (want []testItem) {
				var err error
				// init db
				args.db, err = ddbMain.conn()
				assert.NoError(t, err)

				len := 100

				var id string
				assert.NoError(t, err)

				for i := 0; i < len; i++ {
					// randomize
					o := testItem{}
					err = RandomizeDDBStruct(&o)

					assert.NoError(t, err)

					if i == 0 {
						id = o.HashKey
						// add want
						want = append(want, o)
					}

					// put item
					err = PutItem(args.ctx, args.db, o, expression.Expression{})
					assert.NoError(t, err)

				}
				// set args

				proj := ProjectionAll[testItem]()
				keycond := expression.Key(testItemColumns.HashKey).Equal(expression.Value(id))
				args.expr, err = expression.NewBuilder().WithProjection(proj).WithKeyCondition(keycond).Build()
				args.opts = []QueryOptionFunc{
					func(qo *QueryOptions) {
						qo.Limit = aws.Int32(10)
					},
				}
				assert.NoError(t, err)
				return want
			},
			opts: []cmp.Option{
				cmpopts.SortSlices(func(x, y testItem) bool {
					return x.HashKey < y.HashKey
				}),
				cmpopts.IgnoreUnexported(testItem{}),
			},
		},
		"success with 0 length": {
			args: args{
				ctx: context.Background(),
			},
			setup: func(t *testing.T, args *args) (want []testItem) {
				var err error
				// init db
				args.db, err = ddbMain.conn()
				assert.NoError(t, err)

				id := "testId"
				// set args

				proj := ProjectionAll[testItem]()
				keycond := expression.Key(testItemColumns.HashKey).Equal(expression.Value(id))
				args.expr, err = expression.NewBuilder().WithProjection(proj).WithKeyCondition(keycond).Build()
				args.opts = []QueryOptionFunc{
					func(qo *QueryOptions) {
						qo.Limit = aws.Int32(10)
					},
				}
				assert.NoError(t, err)
				return []testItem{}
			},
			wantErr: false,
			opts: []cmp.Option{
				cmpopts.SortSlices(func(x, y testItem) bool {
					return x.HashKey < y.HashKey
				}),
				cmpopts.IgnoreUnexported(testItem{}),
			},
		},
		"success with gsi": {
			args: args{
				ctx: context.Background(),
			},
			setup: func(t *testing.T, args *args) (want []testItem) {
				var err error
				// init db
				args.db, err = ddbMain.conn()
				assert.NoError(t, err)

				len := 100

				hashkey, err := NewRandomEngStr(28)
				assert.NoError(t, err)

				rangekey, err := NewRandomEngStr(28)
				assert.NoError(t, err)

				for i := 0; i < len; i++ {
					// randomize
					o := testItem{}
					err = RandomizeDDBStruct(&o)

					assert.NoError(t, err)

					// at least 1 item with filter

					if i%2 == 0 {
						o.GSIHashKey = hashkey
						if i%3 == 0 {
							o.GSIRangeKey = rangekey
							want = append(want, o)
						}
					}

					// put item
					err = PutItem(args.ctx, args.db, o, expression.Expression{})
					assert.NoError(t, err)

				}
				// set args

				args.opts = []QueryOptionFunc{
					func(opt *QueryOptions) {
						opt.IndexName = &testItemIndexName.GSI
						opt.Limit = aws.Int32(10)
					},
				}

				proj := ProjectionAll[testItem]()
				keycond := expression.Key(testItemColumns.GSIHashKey).Equal(expression.Value(hashkey)).And(expression.Key(testItemColumns.GSIRangeKey).Equal(expression.Value(rangekey)))
				args.expr, err = expression.NewBuilder().WithProjection(proj).WithKeyCondition(keycond).Build()
				assert.NoError(t, err)
				return want
			},
			opts: []cmp.Option{
				cmpopts.SortSlices(func(x, y testItem) bool {
					return x.HashKey < y.HashKey
				}),

				cmpopts.IgnoreUnexported(testItem{}),
			},
		},
		"success with gsi and filter": {
			args: args{
				ctx: context.Background(),
			},
			setup: func(t *testing.T, args *args) (want []testItem) {
				var err error
				// init db
				args.db, err = ddbMain.conn()
				assert.NoError(t, err)

				len := 100

				filt, err := NewRandomEngStr(10)
				assert.NoError(t, err)

				hashkey, err := NewRandomEngStr(28)
				assert.NoError(t, err)

				rangekey, err := NewRandomEngStr(28)
				assert.NoError(t, err)

				for i := 0; i < len; i++ {
					// randomize
					o := testItem{}
					err = RandomizeDDBStruct(&o)

					assert.NoError(t, err)

					// at least 1 item with filter
					if i == 0 {
						o.FilterKey = filt
						o.GSIRangeKey = rangekey
					}

					if i%3 == 0 {
						o.FilterKey = filt
					}

					if i%2 == 0 {
						o.GSIHashKey = hashkey
						if i%3 == 0 && i%5 == 0 {
							o.GSIRangeKey = rangekey

							// add want
							want = append(want, o)

						}

					}

					// put item
					err = PutItem(args.ctx, args.db, o, expression.Expression{})
					assert.NoError(t, err)

				}
				// set args

				args.opts = []QueryOptionFunc{
					func(opt *QueryOptions) {
						opt.IndexName = &testItemIndexName.GSI
						opt.Limit = aws.Int32(10)
					},
				}

				proj := ProjectionAll[testItem]()
				keycond := expression.Key(testItemColumns.GSIHashKey).Equal(expression.Value(hashkey)).And(expression.Key(testItemColumns.GSIRangeKey).Equal(expression.Value(rangekey)))
				filter := expression.Name(testItemColumns.FilterKey).Equal(expression.Value(filt))
				args.expr, err = expression.NewBuilder().WithProjection(proj).WithKeyCondition(keycond).WithFilter(filter).Build()
				assert.NoError(t, err)
				return want
			},
			opts: []cmp.Option{
				cmpopts.SortSlices(func(x, y testItem) bool {
					return x.HashKey < y.HashKey
				}),

				cmpopts.IgnoreUnexported(testItem{}),
			},
		},
		"empty": {
			args: args{
				ctx: context.Background(),
			},
			setup: func(t *testing.T, args *args) (want []testItem) {
				var err error

				// init db
				args.db, err = ddbMain.conn()
				assert.NoError(t, err)

				// randomize
				o := testItem{}
				err = RandomizeDDBStruct(&o)
				assert.NoError(t, err)
				hashKey, err := NewRandomEngStr(28)
				assert.NoError(t, err)

				// set args

				proj := ProjectionAll[testItem]()
				keycond := expression.Key(testItemColumns.HashKey).Equal(expression.Value(hashKey))
				args.expr, err = expression.NewBuilder().WithProjection(proj).WithKeyCondition(keycond).Build()
				assert.NoError(t, err)

				return []testItem{}
			},
			opts: []cmp.Option{
				cmpopts.SortSlices(func(x, y testItem) bool {
					return x.HashKey < y.HashKey
				}),

				cmpopts.IgnoreUnexported(testItem{}),
			},
			wantErr: false,
		},
	}
	for name, tt := range tests {
		tt := tt
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			tt.want = tt.setup(t, &tt.args)
			got, err := QueryAll[testItem](tt.args.ctx, tt.args.db, tt.args.expr, tt.args.opts...)
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

func testtestItemScan(t *testing.T) {
	type args struct {
		ctx  context.Context
		db   *dynamodb.Client
		expr expression.Expression
		opts []ScanOptionFunc
	}
	tests := map[string]struct {
		args       args
		want       []testItem
		want1      map[string]types.AttributeValue
		setup      func(t *testing.T, args *args) ([]testItem, map[string]types.AttributeValue)
		wantErr    bool
		opts       []cmp.Option
		selfAssert []func(t *testing.T, args args, want []testItem, want1 map[string]types.AttributeValue)
	}{
		"success": {
			args: args{
				ctx: context.Background(),
			},
			setup: func(t *testing.T, args *args) (want []testItem, want1 map[string]types.AttributeValue) {
				var err error
				// init db
				args.db, err = ddbMain.conn()
				assert.NoError(t, err)

				len := int(NewRandomNonZeroInt()%100 + 1)

				assert.NoError(t, err)

				for i := 0; i < len; i++ {
					// randomize
					o := testItem{}
					err = RandomizeDDBStruct(&o)

					assert.NoError(t, err)

					// add want
					want = append(want, o)

					// put item
					err = PutItem(args.ctx, args.db, o, expression.Expression{})
					assert.NoError(t, err)

				}
				// set args

				proj := ProjectionAll[testItem]()
				args.expr, err = expression.NewBuilder().WithProjection(proj).Build()
				assert.NoError(t, err)
				return want, nil
			},
			opts: []cmp.Option{
				cmpopts.SortSlices(func(x, y testItem) bool {
					return x.HashKey < y.HashKey
				}),

				cmpopts.IgnoreUnexported(testItem{}),
			},
		},
		"success with filter": {
			args: args{
				ctx: context.Background(),
			},
			setup: func(t *testing.T, args *args) (want []testItem, want1 map[string]types.AttributeValue) {
				var err error
				// init db
				args.db, err = ddbMain.conn()
				assert.NoError(t, err)

				len := int(NewRandomNonZeroInt()%100 + 1)

				filt, err := NewRandomEngStr(10)
				assert.NoError(t, err)

				for i := 0; i < len; i++ {
					// randomize
					o := testItem{}
					err = RandomizeDDBStruct(&o)

					assert.NoError(t, err)

					if i%2 == 0 {

						o.FilterKey = filt
						// add want
						want = append(want, o)

					}

					// put item
					err = PutItem(args.ctx, args.db, o, expression.Expression{})
					assert.NoError(t, err)

				}
				// set args

				proj := ProjectionAll[testItem]()
				filter := expression.Name(testItemColumns.FilterKey).Equal(expression.Value(filt))
				args.expr, err = expression.NewBuilder().WithProjection(proj).WithFilter(filter).Build()
				assert.NoError(t, err)
				return want, nil
			},
			opts: []cmp.Option{
				cmpopts.SortSlices(func(x, y testItem) bool {
					return x.HashKey < y.HashKey
				}),

				cmpopts.IgnoreUnexported(testItem{}),
			},
		},
		"empty": {
			args: args{
				ctx: context.Background(),
			},
			setup: func(t *testing.T, args *args) (want []testItem, want1 map[string]types.AttributeValue) {
				var err error

				// init db
				args.db, err = ddbMain.conn()
				assert.NoError(t, err)

				// randomize
				o := testItem{}
				err = RandomizeDDBStruct(&o)
				assert.NoError(t, err)
				hashkey, err := NewRandomEngStr(28)
				assert.NoError(t, err)

				// set args

				proj := ProjectionAll[testItem]()
				keycond := expression.Key(testItemColumns.HashKey).Equal(expression.Value(hashkey))
				args.expr, err = expression.NewBuilder().WithProjection(proj).WithKeyCondition(keycond).Build()
				assert.NoError(t, err)

				return nil, nil
			},
			opts: []cmp.Option{
				cmpopts.SortSlices(func(x, y testItem) bool {
					return x.HashKey < y.HashKey
				}),

				cmpopts.IgnoreUnexported(testItem{}),
			},
			wantErr: true,
		},
	}
	for name, tt := range tests {
		tt := tt
		t.Run(name, func(t *testing.T) {
			err := truncateAllTables(t)
			assert.NoError(t, err)
			tt.want, tt.want1 = tt.setup(t, &tt.args)
			got, got1, err := Scan[testItem](tt.args.ctx, tt.args.db, tt.args.expr, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("wantErr is %t, but err is %v", tt.wantErr, err)
			}
			if diff := cmp.Diff(tt.want, got, tt.opts...); len(diff) > 0 {
				t.Errorf("Compare value is mismatch (-want +got):%s\n", diff)
			}
			if diff := cmp.Diff(tt.want1, got1, tt.opts...); len(diff) > 0 {
				t.Errorf("Compare value is mismatch (-want +got):%s\n", diff)
			}
			for _, fn := range tt.selfAssert {
				fn(t, tt.args, tt.want, tt.want1)
			}
		})
	}
}

func testtestItemScanAll(t *testing.T) {
	type args struct {
		ctx  context.Context
		db   *dynamodb.Client
		expr expression.Expression
		opts []ScanOptionFunc
	}
	tests := map[string]struct {
		args       args
		want       []testItem
		setup      func(t *testing.T, args *args) []testItem
		wantErr    bool
		opts       []cmp.Option
		selfAssert []func(t *testing.T, args args, want []testItem)
	}{
		"success": {
			args: args{
				ctx: context.Background(),
			},
			setup: func(t *testing.T, args *args) (want []testItem) {
				var err error
				// init db
				args.db, err = ddbMain.conn()
				assert.NoError(t, err)

				len := 100

				assert.NoError(t, err)

				for i := 0; i < len; i++ {
					// randomize
					o := testItem{}
					err = RandomizeDDBStruct(&o)

					assert.NoError(t, err)

					// add want
					want = append(want, o)

					// put item
					err = PutItem(args.ctx, args.db, o, expression.Expression{})
					assert.NoError(t, err)

				}
				// set args

				proj := ProjectionAll[testItem]()
				args.expr, err = expression.NewBuilder().WithProjection(proj).Build()
				assert.NoError(t, err)
				return want
			},
			opts: []cmp.Option{
				cmpopts.SortSlices(func(x, y testItem) bool {
					return x.HashKey < y.HashKey
				}),

				cmpopts.IgnoreUnexported(testItem{}),
			},
		},
		"success with filter": {
			args: args{
				ctx: context.Background(),
			},
			setup: func(t *testing.T, args *args) (want []testItem) {
				var err error
				// init db
				args.db, err = ddbMain.conn()
				assert.NoError(t, err)

				len := 100

				filt, err := NewRandomEngStr(10)
				assert.NoError(t, err)

				for i := 0; i < len; i++ {
					// randomize
					o := testItem{}
					err = RandomizeDDBStruct(&o)

					assert.NoError(t, err)

					if i%2 == 0 {

						o.FilterKey = filt
						// add want
						want = append(want, o)

					}

					// put item
					err = PutItem(args.ctx, args.db, o, expression.Expression{})
					assert.NoError(t, err)

				}

				// set args

				proj := ProjectionAll[testItem]()
				filter := expression.Name(testItemColumns.FilterKey).Equal(expression.Value(filt))
				args.expr, err = expression.NewBuilder().WithProjection(proj).WithFilter(filter).Build()
				assert.NoError(t, err)
				return want
			},
			opts: []cmp.Option{
				cmpopts.SortSlices(func(x, y testItem) bool {
					return x.HashKey < y.HashKey
				}),

				cmpopts.IgnoreUnexported(testItem{}),
			},
		},
		"empty": {
			args: args{
				ctx: context.Background(),
			},
			setup: func(t *testing.T, args *args) (want []testItem) {
				var err error

				// init db
				args.db, err = ddbMain.conn()
				assert.NoError(t, err)

				// randomize
				o := testItem{}
				err = RandomizeDDBStruct(&o)
				assert.NoError(t, err)
				hashkey, err := NewRandomEngStr(28)
				assert.NoError(t, err)

				// set args

				proj := ProjectionAll[testItem]()
				keycond := expression.Key(testItemColumns.HashKey).Equal(expression.Value(hashkey))
				args.expr, err = expression.NewBuilder().WithProjection(proj).WithKeyCondition(keycond).Build()
				args.opts = []ScanOptionFunc{
					func(opt *ScanOptions) {
						opt.Limit = aws.Int32(10)
					},
				}
				assert.NoError(t, err)

				return nil
			},
			opts: []cmp.Option{
				cmpopts.SortSlices(func(x, y testItem) bool {
					return x.HashKey < y.HashKey
				}),

				cmpopts.IgnoreUnexported(testItem{}),
			},
			wantErr: true,
		},
	}
	for name, tt := range tests {
		tt := tt
		t.Run(name, func(t *testing.T) {
			err := truncateAllTables(t)
			assert.NoError(t, err)
			tt.want = tt.setup(t, &tt.args)
			got, err := ScanAll[testItem](tt.args.ctx, tt.args.db, tt.args.expr, tt.args.opts...)
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
