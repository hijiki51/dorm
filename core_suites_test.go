package dorm

import (
	"testing"
)

func TestGetItem(t *testing.T) {
	t.Parallel()
	t.Run("testItem", testtestItemGetItem)
}

func TestBatchGetItems(t *testing.T) {
	t.Parallel()
	t.Run("testItem", testtestItemBatchGetItems)
}

func TestPutItem(t *testing.T) {
	t.Parallel()
	t.Run("testItem", testtestItemPutItem)
}
func TestBatchPutItem(t *testing.T) {
	t.Parallel()
	t.Run("testItem", testtestItemBatchPutItem)
}

func TestQuery(t *testing.T) {
	t.Parallel()
	t.Run("testItem", testtestItemQuery)
}
func TestQueryAll(t *testing.T) {
	t.Parallel()
	t.Run("testItem", testtestItemQueryAll)
}
func TestUpdateItem(t *testing.T) {
	t.Parallel()
	t.Run("testItem", testtestItemUpdateItem)
}

func TestDeleteItem(t *testing.T) {
	t.Parallel()
	t.Run("testItem", testtestItemDeleteItem)
}

func TestBatchDeleteItem(t *testing.T) {
	t.Parallel()
	t.Run("testItem", testtestItemBatchDeleteItem)
}

// Scan Test Should not run in parallel. It will cause conflict.
func TestScan(t *testing.T) {
	t.Run("testItem", testtestItemScan)
}

func TestScanAll(t *testing.T) {
	t.Run("testItem", testtestItemScanAll)
}
