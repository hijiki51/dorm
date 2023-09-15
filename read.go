package dorm

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/friendsofgo/errors"
)

const batchGetItemsMaxSize = 100

// QueryOptions Queryのオプションパラメータ
type QueryOptions struct {
	IndexName         *string
	ExclusiveStartKey map[string]types.AttributeValue

	Limit   *int32
	Reverse bool
}

// ScanOptions Scanのオプションパラメータ
type ScanOptions struct {
	IndexName         *string
	ExclusiveStartKey map[string]types.AttributeValue

	Limit *int32
}

type ScanOptionFunc func(*ScanOptions)
type QueryOptionFunc func(*QueryOptions)

// GetItem 指定されたアイテムを取得する。
//
// https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb#Client.GetItem
// https://docs.aws.amazon.com/ja_jp/amazondynamodb/latest/APIReference/API_GetItem.html
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

	if err = checkEmptyResp(output.Item); err != nil {
		return nil, ErrItemNotFound
	}

	var val V
	err = attributevalue.UnmarshalMap(output.Item, &val)
	if err != nil {
		return nil, err
	}

	return &val, nil

}

// BatchGetItems 複数アイテムを一括取得する。
//
// AWSの仕様上は複数テーブルにアクセスできるがここでは単一テーブルに制限している。
// 一度にリクエストできる上限は100件。
// https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb#Client.BatchGetItem
// https://docs.aws.amazon.com/ja_jp/amazondynamodb/latest/APIReference/API_BatchGetItem.html
func BatchGetItems[V ItemType](ctx context.Context, db *dynamodb.Client, idxs []PrimaryIndex, expr expression.Expression) ([]V, error) {
	res, errs := splitThreadWithReturnValue(ctx, db, expr, batchGetItemsMaxSize, batchGetItems[V], idxs)

	if len(errs) > 0 {
		return nil, errs[0]
	}

	return res, nil
}

// Query クエリ実行
//
// 注意: AWSの仕様上 KeyCondition => Limit => FilterExpression の順で実行される。
// https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb#Client.Query
// https://docs.aws.amazon.com/ja_jp/amazondynamodb/latest/APIReference/API_Query.html
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

	if err = checkEmptyRespList(output.Items); err != nil {
		return nil, nil, ErrItemNotFound
	}

	var vals []V
	err = attributevalue.UnmarshalListOfMaps(output.Items, &vals)
	if err != nil {
		return nil, nil, err
	}

	return vals, output.LastEvaluatedKey, nil

}

// QueryAll クエリ実行(全件取得)
//
// 注意: AWSの仕様上 KeyCondition => Limit => FilterExpression の順で実行される。
// https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb#Client.Query
// https://docs.aws.amazon.com/ja_jp/amazondynamodb/latest/APIReference/API_Query.html\
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
		return nil, ErrItemNotFound
	}

	return resp, nil
}

// Scan テーブルスキャン
//
// 注意: AWSの仕様上 Limit => FilterExpression の順で実行される。
// https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb#Client.Scan
// https://docs.aws.amazon.com/ja_jp/amazondynamodb/latest/APIReference/API_Scan.html
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

	if err = checkEmptyRespList(output.Items); err != nil {
		return nil, nil, ErrItemNotFound
	}

	var vals []V
	err = attributevalue.UnmarshalListOfMaps(output.Items, &vals)
	if err != nil {
		return nil, nil, err
	}

	return vals, output.LastEvaluatedKey, nil
}

// ScanAll テーブルスキャン(全件取得)
//
// 注意: AWSの仕様上 Limit => FilterExpression の順で実行される。
// https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb#Client.Scan
// https://docs.aws.amazon.com/ja_jp/amazondynamodb/latest/APIReference/API_Scan.html
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
		return nil, errors.Wrap(ErrInternalServerError, "Too Many Items")
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

	if err = checkEmptyRespList(output.Responses[*getFullTableName[V]()]); err != nil {
		return nil, ErrItemNotFound
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