package dorm

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func createtestItemTable(db *dynamodb.Client) error {
	ctx := context.Background()
	v := testItem{}
	tn := v.TableName()
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String(testItemColumns.HashKey),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(testItemColumns.GSIHashKey),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(testItemColumns.GSIRangeKey),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(testItemColumns.HashKey),
				KeyType:       types.KeyTypeHash,
			},
		},
		TableName: aws.String(tn),
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String(testItemIndexName.GSI),
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String(testItemColumns.GSIHashKey),
						KeyType:       types.KeyTypeHash,
					},
					{
						AttributeName: aws.String(testItemColumns.GSIRangeKey),
						KeyType:       types.KeyTypeRange,
					},
				},
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				},
				ProvisionedThroughput: &types.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(1000),
					WriteCapacityUnits: aws.Int64(1000),
				},
			},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1000),
			WriteCapacityUnits: aws.Int64(1000),
		},
	}

	// create table
	_, err := db.CreateTable(ctx, input)

	return err

}
