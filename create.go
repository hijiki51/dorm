package dorm

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const maxBatchPutItemSize = 25

// BatchPutItemOptions BatchPutItem options for BatchPutItem function
type BatchPutItemOptions struct {
	Concurrency int
}

// BatchPutOptionFunc BatchPutItem option function
type BatchPutOptionFunc func(*BatchPutItemOptions)

// WithBatchPutConcurrency sets the Concurrency for BatchPutItemOptions.
func WithBatchPutConcurrency(concurrency int) BatchPutOptionFunc {
	return func(opts *BatchPutItemOptions) {
		opts.Concurrency = concurrency
	}
}

// PutItem adds an item if it doesn't exist, or replaces it if it does.
//
// Be careful, as it will overwrite with zero values if set.
// Use UpdateItem if you want to update.
//
// https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb#Client.PutItem
// https://docs.aws.amazon.com/en_us/amazondynamodb/latest/APIReference/API_PutItem.html
func PutItem[V ItemType](ctx context.Context, db *dynamodb.Client, item V, expr expression.Expression) error {

	av, err := attributevalue.MarshalMap(item)

	if err != nil {
		return err
	}

	input := &dynamodb.PutItemInput{
		Item:                      av,
		TableName:                 getFullTableName[V](),
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ReturnValues:              types.ReturnValueNone,
	}

	_, err = db.PutItem(ctx, input)

	if err != nil {
		return err
	}

	return nil

}

// BatchPutItem adds multiple items in bulk.
//
// It is limited to a single table, although AWS allows accessing multiple tables.
// Also, it can perform a mix of deletion and creation, but here it is restricted to a single operation.
// https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb#Client.BatchWriteItem
// https://docs.aws.amazon.com/en_us/amazondynamodb/latest/APIReference/API_BatchWriteItem.html
func BatchPutItem[V ItemType](ctx context.Context, db *dynamodb.Client, items []V, opts ...BatchPutOptionFunc) error {

	o := BatchPutItemOptions{}

	for _, f := range opts {
		f(&o)
	}

	if len(items) == 0 {
		return nil
	}
	// The number of operations that can be performed in a single batch is up to 25
	err := splitThread(ctx, db, NopExpression, maxBatchPutItemSize, o.Concurrency, batchPutItem[V], items)

	if err != nil {
		return err
	}

	return nil

}

func batchPutItem[V ItemType](ctx context.Context, db *dynamodb.Client, expr expression.Expression, items []V) error {

	if len(items) == 0 {
		return nil
	}

	writeReqs := make([]types.WriteRequest, len(items))
	for i, item := range items {
		av, err := attributevalue.MarshalMap(item)
		if err != nil {
			return err
		}

		writeReqs[i] = types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: av,
			},
		}

	}

	_, err := db.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			*getFullTableName[V](): writeReqs,
		},
	})

	if err != nil {
		return err
	}

	return nil

}
