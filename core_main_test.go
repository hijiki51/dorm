package dorm

import (
	"fmt"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/ory/dockertest/v3"
)

var (
	ddbMain    tester
	ddbClient  *dynamodb.Client
	containers []*dockertest.Resource
)

type tester interface {
	setup(*dockertest.Resource) error
	create(*dynamodb.Client) error
	conn() (*dynamodb.Client, error)
	teardown() error
}

func TestMain(m *testing.M) {
	if ddbMain == nil {
		fmt.Println("no ddbMain tester interface was ready")
		os.Exit(-1)
	}

	var err error

	pool, err := dockertest.NewPool("")

	if err != nil {
		fmt.Println("Unable to create docker pool:", err)
		os.Exit(-1)
	}

	dc, err := setupDDBDocker(pool)

	if err != nil {
		fmt.Println("Unable to setup docker:", err)
		os.Exit(-2)
	}

	if err = ddbMain.setup(dc); err != nil {
		fmt.Println("Unable to execute setup:", err)
		os.Exit(-4)
	}

	conn, err := ddbMain.conn()
	if err != nil {
		fmt.Println("failed to get connection:", err)
	}

	var code int
	ddbClient = conn

	err = ddbMain.create(conn)

	if err != nil {
		fmt.Println("failed to create table:", err)
	}

	code = m.Run()

	if err = ddbMain.teardown(); err != nil {
		fmt.Println("Unable to execute teardown:", err)
		os.Exit(-5)
	}

	if err = removeDocker(pool); err != nil {
		fmt.Println("Unable to remove docker:", err)
		os.Exit(-3)
	}

	os.Exit(code)
}

func setupDDBDocker(pool *dockertest.Pool) (*dockertest.Resource, error) {

	dc, err := setupDynamoContainer(pool)

	if err != nil {
		fmt.Println("Unable to setup dynamo container:", err)
		return nil, err
	}

	containers = append(containers, dc)

	return dc, nil
}

func removeDocker(pool *dockertest.Pool) error {
	for _, c := range containers {
		if err := pool.Purge(c); err != nil {
			return err
		}
	}

	return nil
}
