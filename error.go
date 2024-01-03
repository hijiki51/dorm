package dorm

import "github.com/cockroachdb/errors"

// nolint
var (
	ErrRequirePrimaryKey = errors.New("If set GSI key, it also requires Primary key")
	// ErrItemNotFound Item Not Found error
	ErrItemNotFound = errors.New("Item not found")
	// ErrMaxGetItemExceeded Max GetItem Exceeded error
	ErrMaxGetItemExceeded = errors.New("Max GetItem Exceeded")
)
