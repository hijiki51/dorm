package dorm

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const maxBatchDeleteSize = 25

type BatchDeleteItemOptions struct {
	Concurrency int
}

type BatchDeleteOptionFunc func(*BatchDeleteItemOptions)

// DeleteItem deletes an item.
// https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb#Client.DeleteItem
func DeleteItem[V ItemType](ctx context.Context, db *dynamodb.Client, idx PrimaryIndex, expr expression.Expression) error {

	key, err := buildIndex(idx)
	if err != nil {
		return err
	}

	input := &dynamodb.DeleteItemInput{
		Key:                       key,
		TableName:                 getFullTableName[V](),
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ReturnValues:              types.ReturnValueNone,
	}

	_, err = db.DeleteItem(ctx, input)

	return err

}

// BatchDeleteItem adds items in bulk.
//
// According to AWS specifications, it is possible to access multiple tables, but here we are limiting it to a single table.
// Also, deletion and creation can be mixed, but we are limiting it to a single operation.
// https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb#Client.BatchWriteItem
// https://docs.aws.amazon.com/en_us/amazondynamodb/latest/APIReference/API_BatchWriteItem.html
func BatchDeleteItem[V ItemType](ctx context.Context, db *dynamodb.Client, keys []PrimaryIndex, opts ...BatchDeleteOptionFunc) error {

	o := BatchDeleteItemOptions{}

	for _, f := range opts {
		f(&o)
	}

	if len(keys) == 0 {
		return nil
	}
	// The number of operations that can be performed in a single batch is up to 25.
	err := splitThread(ctx, db, NopExpression, maxBatchPutItemSize, o.Concurrency, batchDeleteItem[V], keys)

	if err != nil {
		return err
	}

	return nil

}

func batchDeleteItem[V ItemType](ctx context.Context, db *dynamodb.Client, expr expression.Expression, keys []PrimaryIndex) error {
	// The number of operations that can be performed in a single batch is up to 25.

	writeReqs := make([]types.WriteRequest, len(keys))
	for i, item := range keys {
		av, err := attributevalue.MarshalMap(item)
		if err != nil {
			return err
		}
		writeReqs[i] = types.WriteRequest{
			DeleteRequest: &types.DeleteRequest{
				Key: av,
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
