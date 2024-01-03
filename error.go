package dorm

import "github.com/cockroachdb/errors"

// nolint
var (
	ErrRequirePrimaryKey = errors.New("If set GSI key, it also requires Primary key")
	// ErrItemNotFound Item Not Found error
	ErrTableNotFound = errors.New("Undefined table")
	// ErrTableNotFound Table Not Found error
	ErrItemNotFound = errors.New("Item not found")
	// ErrInvalidBatchWriteItemRequestType Invalid BatchWriteItem Request Type
	ErrInvalidBatchWriteItemRequestType = errors.New("Invalid BatchWriteItem Request Type")
	// ErrInternalServerError Internal Server Error
	ErrInternalServerError = errors.New("Internal Server Error")
)
