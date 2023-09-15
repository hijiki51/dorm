package dorm

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// NewClient 新しいDynamoDBのクライアントを作成する
func NewClient(cfg aws.Config) *dynamodb.Client {
	client := dynamodb.NewFromConfig(cfg)
	return client
}
