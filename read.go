package dorm

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const batchGetItemsMaxSize = 100

// QueryOptions Query options for Query function
type QueryOptions struct {
	IndexName         *string
	ExclusiveStartKey map[string]types.AttributeValue

	Limit   *int32
	Reverse bool
}

// ScanOptions Scan options for Scan function
type ScanOptions struct {
	IndexName         *string
	ExclusiveStartKey map[string]types.AttributeValue

	Limit *int32
}

// BatchGetItemOptions BatchGetItem options for BatchGetItem function
type BatchGetItemOptions struct {
	Concurrency int
}

// ScanOptionFunc Scan option function
type ScanOptionFunc func(*ScanOptions)
// QueryOptionFunc Query option function
type QueryOptionFunc func(*QueryOptions)
// BatchGetItemOptionFunc BatchGetItem option function
type BatchGetItemOptionFunc func(*BatchGetItemOptions)

// WithIndexName sets the IndexName for QueryOptions.
func WithIndexName(name string) QueryOptionFunc {
    return func(opts *QueryOptions) {
        opts.IndexName = &name
    }
}

// WithExclusiveStartKey sets the ExclusiveStartKey for QueryOptions.
func WithExclusiveStartKey(key map[string]types.AttributeValue) QueryOptionFunc {
    return func(opts *QueryOptions) {
        opts.ExclusiveStartKey = key
    }
}

// WithLimit sets the Limit for QueryOptions.
func WithLimit(limit int32) QueryOptionFunc {
    return func(opts *QueryOptions) {
        opts.Limit = &limit
    }
}

// WithReverse sets the Reverse flag for QueryOptions.
func WithReverse(reverse bool) QueryOptionFunc {
    return func(opts *QueryOptions) {
        opts.Reverse = reverse
    }
}

// WithScanIndexName sets the IndexName for ScanOptions.
func WithScanIndexName(name string) ScanOptionFunc {
    return func(opts *ScanOptions) {
        opts.IndexName = &name
    }
}

// WithScanExclusiveStartKey sets the ExclusiveStartKey for ScanOptions.
func WithScanExclusiveStartKey(key map[string]types.AttributeValue) ScanOptionFunc {
    return func(opts *ScanOptions) {
        opts.ExclusiveStartKey = key
    }
}

// WithScanLimit sets the Limit for ScanOptions.
func WithScanLimit(limit int32) ScanOptionFunc {
    return func(opts *ScanOptions) {
        opts.Limit = &limit
    }
}

// WithBatchGetConcurrency sets the concurrency for BatchGetItemOptions.
func WithBatchGetConcurrency(concurrency int) BatchGetItemOptionFunc {
	return func(opts *BatchGetItemOptions) {
		opts.Concurrency = concurrency
	}
}

// GetItem retrieves the specified item.
//
// https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb#Client.GetItem
// https://docs.aws.amazon.com/en_us/amazondynamodb/latest/APIReference/API_GetItem.html
func GetItem[V ItemType](ctx context.Context, db *dynamodb.Client, idx PrimaryIndex, expr expression.Expression) (*V, error) {

	key, err := buildIndex(idx)
	if err != nil {
		return nil, err
	}

	input := &dynamodb.GetItemInput{
		Key:                      key,
		TableName:                getFullTableName[V](),
		ExpressionAttributeNames: expr.Names(),
		ProjectionExpression:     expr.Projection(),
	}

	output, err := db.GetItem(ctx, input)

	if err != nil {
		return nil, err
	}

	if checkEmptyResp(output.Item) {
		return nil, ErrItemNotFound
	}

	var val V
	err = attributevalue.UnmarshalMap(output.Item, &val)
	if err != nil {
		return nil, err
	}

	return &val, nil

}

// BatchGetItems retrieves multiple items in a batch.
//
// Although AWS allows accessing multiple tables, this function is limited to a single table.
// The maximum number of items that can be requested at once is 100.
// https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb#Client.BatchGetItem
// https://docs.aws.amazon.com/en_us/amazondynamodb/latest/APIReference/API_BatchGetItem.html
func BatchGetItems[V ItemType](ctx context.Context, db *dynamodb.Client, idxs []PrimaryIndex, expr expression.Expression, opts ...BatchGetItemOptionFunc) ([]V, error) {
	o := BatchGetItemOptions{}

	for _, f := range opts {
		f(&o)
	}

	res, err := splitThreadWithReturnValue(ctx, db, expr, batchGetItemsMaxSize, o.Concurrency, batchGetItems[V], idxs)

	if err != nil {
		return nil, err
	}

	return res, nil
}

// Query executes a query.
//
// Note: According to AWS specifications, KeyCondition => Limit => FilterExpression are executed in order.
// https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb#Client.Query
// https://docs.aws.amazon.com/en_us/amazondynamodb/latest/APIReference/API_Query.html
func Query[V ItemType](ctx context.Context, db *dynamodb.Client, expr expression.Expression, opts ...QueryOptionFunc) ([]V, map[string]types.AttributeValue, error) {

	o := QueryOptions{}

	for _, f := range opts {
		f(&o)
	}

	input := &dynamodb.QueryInput{
		TableName:                 getFullTableName[V](),
		ExclusiveStartKey:         o.ExclusiveStartKey,
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		IndexName:                 o.IndexName,
		Limit:                     o.Limit,
		KeyConditionExpression:    expr.KeyCondition(),
		ProjectionExpression:      expr.Projection(),
		Select:                    types.SelectSpecificAttributes,
		ScanIndexForward:          aws.Bool(o.Reverse),
	}

	output, err := db.Query(ctx, input)

	if err != nil {
		return nil, nil, err
	}

	if checkEmptyRespList(output.Items) {
		return []V{}, nil, nil
	}

	var vals []V
	err = attributevalue.UnmarshalListOfMaps(output.Items, &vals)
	if err != nil {
		return nil, nil, err
	}

	return vals, output.LastEvaluatedKey, nil

}

// QueryAll executes a query to retrieve all items.
//
// Note: According to AWS specifications, KeyCondition => Limit => FilterExpression are executed in order.
// https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb#Client.Query
// https://docs.aws.amazon.com/en_us/amazondynamodb/latest/APIReference/API_Query.html
func QueryAll[V ItemType](ctx context.Context, db *dynamodb.Client, expr expression.Expression, opts ...QueryOptionFunc) ([]V, error) {
	var resp []V

	iopts := opts

	for {
		v, lastKey, err := Query[V](ctx, db, expr, iopts...)
		if err != nil {
			return nil, err
		}

		resp = append(resp, v...)

		if len(lastKey) == 0 {
			break
		}

		iopts = append(iopts, func(o *QueryOptions) {
			o.ExclusiveStartKey = lastKey
		})
	}

	if len(resp) == 0 {
		return []V{}, nil
	}

	return resp, nil
}

// Scan performs a table scan.
//
// Note: According to AWS specifications, Limit => FilterExpression are executed in order.
// https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb#Client.Scan
// https://docs.aws.amazon.com/en_us/amazondynamodb/latest/APIReference/API_Scan.html
func Scan[V ItemType](ctx context.Context, db *dynamodb.Client, expr expression.Expression, opts ...ScanOptionFunc) ([]V, map[string]types.AttributeValue, error) {
	o := ScanOptions{}

	for _, f := range opts {
		f(&o)
	}

	input := &dynamodb.ScanInput{
		TableName:                 getFullTableName[V](),
		ExclusiveStartKey:         o.ExclusiveStartKey,
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		IndexName:                 o.IndexName,
		Limit:                     o.Limit,
		ProjectionExpression:      expr.Projection(),
		Select:                    types.SelectSpecificAttributes,
	}

	output, err := db.Scan(ctx, input)

	if err != nil {
		return nil, nil, err
	}

	if checkEmptyRespList(output.Items) {
		return []V{}, nil, nil
	}

	var vals []V
	err = attributevalue.UnmarshalListOfMaps(output.Items, &vals)
	if err != nil {
		return nil, nil, err
	}

	return vals, output.LastEvaluatedKey, nil
}

// ScanAll performs a table scan to retrieve all items.
//
// Note: According to AWS specifications, Limit => FilterExpression are executed in order.
// https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb#Client.Scan
// https://docs.aws.amazon.com/en_us/amazondynamodb/latest/APIReference/API_Scan.html
func ScanAll[V ItemType](ctx context.Context, db *dynamodb.Client, expr expression.Expression, opts ...ScanOptionFunc) ([]V, error) {
	var resp []V

	iopts := opts

	for {
		v, lastKey, err := Scan[V](ctx, db, expr, iopts...)
		if err != nil {
			return nil, err
		}

		resp = append(resp, v...)

		if len(lastKey) == 0 {
			break
		}

		iopts = append(iopts, func(o *ScanOptions) {
			o.ExclusiveStartKey = lastKey
		})
	}

	if len(resp) == 0 {
		return []V{}, nil
	}

	return resp, nil
}

func batchGetItems[V ItemType](ctx context.Context, db *dynamodb.Client, expr expression.Expression, idxs []PrimaryIndex) ([]V, error) {

	if len(idxs) == 0 {
		return []V{}, nil
	}

	if len(idxs) > 100 {
		return nil, ErrMaxGetItemExceeded
	}

	var keys []map[string]types.AttributeValue
	for _, idx := range idxs {
		key, err := buildIndex(idx)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}

	req := make(map[string]types.KeysAndAttributes, 0)
	req[*getFullTableName[V]()] = types.KeysAndAttributes{
		Keys:                     keys,
		ExpressionAttributeNames: expr.Names(),
		ProjectionExpression:     expr.Projection(),
	}

	input := &dynamodb.BatchGetItemInput{RequestItems: req}

	output, err := db.BatchGetItem(ctx, input)

	if err != nil {
		return nil, err
	}

	if checkEmptyRespList(output.Responses[*getFullTableName[V]()]) {
		return []V{}, nil
	}

	var res []V

	for _, item := range output.Responses[*getFullTableName[V]()] {
		var val V
		err = attributevalue.UnmarshalMap(item, &val)
		if err != nil {
			return nil, err
		}
		res = append(res, val)
	}

	return res, nil
}
