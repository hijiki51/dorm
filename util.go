package dorm

import (
	"context"
	"math"
	"reflect"
	"sync"

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

// ProjectionAll constructs a ProjectionBuilder that returns all values of a struct, with the ability to adjust the fields specified by optfns.
//
// optFns: A function that specifies the fields to be expanded. Exclude when returning true.
func ProjectionAll[T ItemType](skipper ...func(name string) bool) expression.ProjectionBuilder {
	// Create the entity to search for tags
	str := *new(T)

	// Get the type information of the struct
	rtStr := reflect.TypeOf(str)

	var names []expression.NameBuilder
	// Loop through all fields of the struct
	for i := 0; i < rtStr.NumField(); i++ {
		// Get field information
		f := rtStr.Field(i)
		// Get tag information
		name := f.Tag.Get(structTag)
		// If the tag is set and not "-", add it
		if name != "" && name != ignoreStructTag {
			if !isSkip(skipper, name) {
				// Convert the value of the tag to NameBuilder
				names = append(names, expression.Name(name))
			}
		} else {
			continue
		}
	}
	// Convert NameBuilder slice to ProjectionBuilder
	res := expression.NamesList(names[0], names[1:]...)

	return res
}

// AttributeSetAll constructs an UpdateBuilder that updates all values of a struct, with the ability to adjust the fields specified by optfns.
//
// optFns: A function that specifies the fields to be expanded. Exclude when returning true.
func AttributeSetAll[T ItemType](str T, skipper ...func(name string) bool) expression.UpdateBuilder {
	// Get the type information of the struct
	rtStr := reflect.TypeOf(str)
	res := expression.UpdateBuilder{}
	// Loop through all fields of the struct
	for i := 0; i < rtStr.NumField(); i++ {
		// Get field information
		f := rtStr.Field(i)
		// Get tag information
		name := f.Tag.Get(structTag)
		// If the tag is set and not "-", add it
		if name != "" && name != ignoreStructTag {
			if !isSkip(skipper, name) {
				// Get the value of the specified field in the struct
				val := reflect.ValueOf(str).Field(i)
				// Add the tag as key and the val as interface{} to the UpdateBuilder
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
	var mu sync.Mutex

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
			mu.Lock()
			res = append(res, val...)
			mu.Unlock()
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return res, nil
}
