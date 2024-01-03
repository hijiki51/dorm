package dorm

import (
	"context"
	"math"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const maxBatchPutItemSize = 25

type BatchPutItemOptions struct {
	Concurrency int
}

type BatchPutOptionFunc func(*BatchPutItemOptions)

// PutItem アイテムが存在しなかった場合は追加、存在した場合は置換する
//
// ゼロ値が設定されていた場合それに書き換えられてしまうので注意。
// 更新したい場合はUpdateItemを使うこと。
//
// https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb#Client.PutItem
// https://docs.aws.amazon.com/ja_jp/amazondynamodb/latest/APIReference/API_PutItem.html
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

// BatchPutItem 一括でアイテムを追加する
//
// AWSの仕様上は複数テーブルにアクセスできるがここでは単一テーブルに制限している。
// また、削除と作成も混合して実行できるが、単一操作にに制限している。
// https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb#Client.BatchWriteItem
// https://docs.aws.amazon.com/ja_jp/amazondynamodb/latest/APIReference/API_BatchWriteItem.html
func BatchPutItem[V ItemType](ctx context.Context, db *dynamodb.Client, items []V, opts ...BatchPutOptionFunc) error {

	o := BatchPutItemOptions{
		Concurrency: math.MaxInt,
	}

	for _, f := range opts {
		f(&o)
	}

	if len(items) == 0 {
		return nil
	}
	// 一回のBatchで操作できる数は25個まで
	errs := splitThread(ctx, db, NopExpression, maxBatchPutItemSize, o.Concurrency, batchPutItem[V], items)

	if len(errs) > 0 {
		return errs[0]
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
