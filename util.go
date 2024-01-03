package dorm

import (
	"context"
	"math"
	"reflect"

	"golang.org/x/sync/errgroup"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

const structTag = "dynamodbav"
const ignoreStructTag = "-"

var NopExpression = expression.Expression{}
var UniqueLimitQueryOptFunc = func(qo *QueryOptions) {
	qo.Limit = aws.Int32(1)
}

// ProjectionAll 構造体のすべての値を返すProjectionを構築する。
// optfnsによって指定するフィールドを調整できる。
//
// optFns : 展開するフィールドを指定する関数。trueを返すとき除外する。
func ProjectionAll[T ItemType](skipper ...func(name string) bool) expression.ProjectionBuilder {
	// tagを検索するのに実体が必要なので作成
	str := *new(T)

	// 構造体の型情報を取得
	rtStr := reflect.TypeOf(str)

	var names []expression.NameBuilder
	// 構造体の全フィールドを取得するループ
	for i := 0; i < rtStr.NumField(); i++ {
		// フィールド情報を取得
		f := rtStr.Field(i)
		// tag情報を取得
		name := f.Tag.Get(structTag)
		// tagが設定されており、かつ `-` でない場合
		if name != "" && name != ignoreStructTag {
			if !isSkip(skipper, name) {
				// tagの値をNameBuilderに変換
				names = append(names, expression.Name(name))
			}
		} else {
			continue
		}
	}
	// NameBuilderのSliceをProjectionBuilderに変換
	res := expression.NamesList(names[0], names[1:]...)

	return res
}

// AttributeSetAll 構造体のすべての値をUpdateするUpdateBuilderを構築する。
// optfnsによって指定するフィールドを調整できる。
//
// optFns : 展開するフィールドを指定する関数。trueを返すとき除外する。
func AttributeSetAll[T ItemType](str T, skipper ...func(name string) bool) expression.UpdateBuilder {
	// 構造体の型情報を取得
	rtStr := reflect.TypeOf(str)
	res := expression.UpdateBuilder{}
	// 構造体の全フィールドを取得するループ
	for i := 0; i < rtStr.NumField(); i++ {
		// フィールド情報を取得
		f := rtStr.Field(i)
		// tag情報を取得
		name := f.Tag.Get(structTag)
		// tagが設定されており、かつ `-` でない場合
		if name != "" && name != ignoreStructTag {
			if !isSkip(skipper, name) {
				// 構造体の指定Fieldの値を取得
				val := reflect.ValueOf(str).Field(i)
				// tagをkey,valをinterface{}に変換しつつUpdateBuilderに追加
				res = res.Set(expression.Name(name), expression.Value(val.Interface()))
			}
		} else {
			continue
		}
	}

	return res
}

func isSkip(f []func(string) bool, name string) bool {
	for _, fn := range f {
		if fn(name) {
			return true
		}
	}
	return false
}

func splitThread[ARG any](
	ctx context.Context,
	db *dynamodb.Client,
	expr expression.Expression,
	size int,
	concurrency int,
	fun func(context.Context, *dynamodb.Client, expression.Expression, []ARG) error,
	args []ARG,
) error {
	threadnum := (len(args) / size) + 1
	eg, ctx := errgroup.WithContext(ctx)
	if concurrency > 0 {
		eg.SetLimit(concurrency)
	}

	for i := 0; i < threadnum; i++ {
		start := i * size
		end := int(math.Min(float64((i+1)*size), float64(len(args))))
		if start >= end {
			continue
		}
		subArgs := args[start:end]
		eg.Go(func() error {
			return fun(ctx, db, expr, subArgs)
		})
	}

	if err := eg.Wait(); err != nil {
		return err
	}

	return nil
}

func splitThreadWithReturnValue[V ItemType, ARG any](
	ctx context.Context,
	db *dynamodb.Client,
	expr expression.Expression,
	size int,
	concurrency int,
	fun func(context.Context, *dynamodb.Client, expression.Expression, []ARG) ([]V, error),
	args []ARG,
) ([]V, error) {
	threadnum := (len(args) / size) + 1
	eg, ctx := errgroup.WithContext(ctx)
	if concurrency > 0 {
		eg.SetLimit(concurrency)
	}
	res := make([]V, 0, len(args))

	for i := 0; i < threadnum; i++ {
		start := i * size
		end := int(math.Min(float64(((i + 1) * size)), float64(len(args))))
		if start >= end {
			continue
		}
		subArgs := args[start:end]
		eg.Go(func() error {
			val, err := fun(ctx, db, expr, subArgs)
			if err != nil {
				return err
			}
			res = append(res, val...)
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return res, nil
}
