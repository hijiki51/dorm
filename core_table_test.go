package dorm

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/stretchr/testify/assert"
)

type createTableFunc func(*dynamodb.Client) error

var funcs = []createTableFunc{
	createtestItemTable,
}

func (d *ddbTester) createTestDB(db *dynamodb.Client) error {

	for _, f := range funcs {
		if err := f(db); err != nil {
			return err
		}
	}

	return nil
}

func truncateAllTables(t *testing.T) error {

	db, err := ddbMain.conn()
	assert.NoError(t, err)

	li := dynamodb.ListTablesInput{
		Limit: aws.Int32(100),
	}

	output, err := db.ListTables(context.Background(), &li)

	assert.NoError(t, err)

	for _, v := range output.TableNames {
		_, err := db.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
			TableName: aws.String(v),
		})

		assert.NoError(t, err)
	}

	for _, f := range funcs {
		if err := f(db); err != nil {
			return err
		}
	}

	return nil
}
