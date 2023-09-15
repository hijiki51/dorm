package dorm

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

func setupDynamoContainer(pool *dockertest.Pool) (*dockertest.Resource, error) {
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "amazon/dynamodb-local",
		Tag:        "latest",
		PortBindings: map[docker.Port][]docker.PortBinding{
			"8000/tcp": {{HostPort: "11001"}},
		},
	}, func(config *docker.HostConfig) {

		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{
			Name: "no",
		}
	})

	if err != nil {
		return nil, err
	}

	expire, err := strconv.Atoi(os.Getenv("TEST_DOCKER_EXPIRE"))

	if err != nil || expire == 0 {
		expire = 120
	}

	if err = resource.Expire(uint(expire)); err != nil {
		return nil, err
	}

	if err = pool.Retry(func() error {

		cfg := dynamoTestConfig(context.Background(), resource.GetPort("8000/tcp"))

		cli := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
			o.RetryMaxAttempts = 3
			o.Retryer = retry.NewStandard(func(opts *retry.StandardOptions) {
				opts.Retryables = append([]retry.IsErrorRetryable{RetryableConnectionError{}}, opts.Retryables...)
			})
		})

		_, err := cli.ListTables(context.Background(), &dynamodb.ListTablesInput{})

		if err != nil {
			fmt.Println("Error connecting to dynamodb: ", err.Error())
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return resource, nil
}
