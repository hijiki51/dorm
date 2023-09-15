package dorm

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// UpdateItem アイテムを更新する
//
// https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb#Client.UpdateItem
// https://docs.aws.amazon.com/ja_jp/amazondynamodb/latest/APIReference/API_UpdateItem.html
func UpdateItem[V ItemType](ctx context.Context, db *dynamodb.Client, idx PrimaryIndex, expr expression.Expression) (*V, error) {

	key, err := buildIndex(idx)
	if err != nil {
		return nil, err
	}

	input := &dynamodb.UpdateItemInput{
		Key:                       key,
		TableName:                 getFullTableName[V](),
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
		ReturnValues:              types.ReturnValueAllNew,
	}

	output, err := db.UpdateItem(ctx, input)

	if err != nil {
		return nil, err
	}

	var val V
	err = attributevalue.UnmarshalMap(output.Attributes, &val)
	if err != nil {
		return nil, err
	}

	return &val, nil

}
