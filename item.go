package dorm

import "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

type Item interface {
	isItem()
}

type ItemType interface {
	Item
	TableName() string
}

func checkEmptyResp(m map[string]types.AttributeValue) error {
	if len(m) == 0 {
		return ErrItemNotFound
	}
	return nil
}

func checkEmptyRespList(m []map[string]types.AttributeValue) error {
	if len(m) == 0 {
		return ErrItemNotFound
	}
	return nil
}
