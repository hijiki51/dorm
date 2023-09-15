package dorm

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/ory/dockertest/v3"
)

type ddbTester struct {
	client *dynamodb.Client
	conf   aws.Config
}

func init() {
	ddbMain = &ddbTester{}
}

func (d *ddbTester) setup(resource *dockertest.Resource) error {

	conf := dynamoTestConfig(context.Background(), resource.GetPort("8000/tcp"))

	d.conf = conf

	return nil
}

func (d *ddbTester) teardown() error {
	return d.dropTestDB()
}

func (d *ddbTester) conn() (*dynamodb.Client, error) {

	if d.client != nil {
		return d.client, nil
	}

	client := dynamodb.NewFromConfig(d.conf, func(o *dynamodb.Options) {
		o.RetryMaxAttempts = 3
		o.Retryer = retry.NewStandard(func(opts *retry.StandardOptions) {
			opts.Retryables = append([]retry.IsErrorRetryable{RetryableConnectionError{}}, opts.Retryables...)
		})
	})

	d.client = client
	return d.client, nil
}

func (d *ddbTester) create(db *dynamodb.Client) error {

	return d.createTestDB(db)
}

func (d *ddbTester) dropTestDB() error {
	li := dynamodb.ListTablesInput{
		Limit: aws.Int32(100),
	}

	output, err := d.client.ListTables(context.Background(), &li)

	if err != nil {
		return err
	}

	for _, v := range output.TableNames {
		_, err := d.client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
			TableName: aws.String(v),
		})

		if err != nil {
			return err
		}
	}
	return nil
}

func dynamoTestConfig(ctx context.Context, port string) aws.Config {

	ep := fmt.Sprintf("http://localhost:%s", port)
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("ap-northeast-1"),
		config.WithEndpointResolverWithOptions(
			aws.EndpointResolverWithOptionsFunc(
				func(service string, region string, options ...interface{}) (aws.Endpoint, error) {
					return aws.Endpoint{
						URL:           ep,
						SigningRegion: region,
					}, nil
				},
			),
		),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("AKID", "SECRET", "")),
	)

	if err != nil {
		panic(err)
	}
	return cfg

}
