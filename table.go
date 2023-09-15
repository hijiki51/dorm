package dorm

import "github.com/aws/aws-sdk-go-v2/aws"

func getFullTableName[T ItemType]() *string {
	v := *new(T)

	return aws.String(v.TableName())
}
