package dorm

import (
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// index represents each index in DynamoDB
type index interface {
	isIndex()
}

type PrimaryIndex interface {
	index
	isPrimaryIndex()
}

type GlobalSecondaryIndex interface {
	index
	isGlobalSecondaryIndex()
}

type LocalSecondaryIndex interface {
	index
	isLocalSecondaryIndex()
}

type IndexType interface {
	index
}

func buildIndex[T IndexType](i T) (map[string]types.AttributeValue, error) {
	v, err := attributevalue.MarshalMap(i)
	if err != nil {
		return nil, err
	}
	return v, nil
}

// ConstructStartKeyWithGSI constructs the StartKey from PrimaryIndex and GSI.
func ConstructStartKeyWithGSI(p PrimaryIndex, s GlobalSecondaryIndex) (map[string]types.AttributeValue, error) {
	res, err := buildIndex(p)

	if err != nil {
		return nil, err
	}

	// If only PrimaryIndex is provided, return as is
	if s == nil {
		return res, nil
	}

	smap, err := buildIndex(s)

	if err != nil {
		return nil, err
	}

	// Merge the keys of Primary and Secondary
	for k, v := range smap {
		res[k] = v
	}

	return res, nil
}

func ConstructStartKey(p PrimaryIndex) (map[string]types.AttributeValue, error) {
	return buildIndex(p)
}
