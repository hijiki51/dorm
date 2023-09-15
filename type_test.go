package dorm

// testItemTableName Name of test Table
const testItemTableName = "test-item"

// testItemIndexName is helper for index
var testItemIndexName = struct {
	GSI string
}{
	GSI: "grobal-secondary-index",
}

// testItemColumns is helper for expression
var testItemColumns = struct {
	HashKey      string
	GSIHashKey   string
	GSIRangeKey  string
	FilterKey    string
	Str          string
	Int8         string
	Int16        string
	Int          string
	Int32        string
	Int64        string
	Uint8        string
	Uint16       string
	Uint         string
	Uint32       string
	Uint64       string
	Float32      string
	Float64      string
	Bool         string
	StrSlice     string
	Int8Slice    string
	Int16Slice   string
	IntSlice     string
	Int32Slice   string
	Int64Slice   string
	Float32Slice string
	Float64Slice string
	BoolSlice    string
}{
	HashKey:      "hash_key",
	GSIHashKey:   "gsi_hash_key",
	GSIRangeKey:  "gsi_range_key",
	FilterKey:    "filter_key",
	Str:          "str",
	Int8:         "int8",
	Int16:        "int16",
	Int:          "int",
	Int32:        "int32",
	Int64:        "int64",
	Uint8:        "uint8",
	Uint16:       "uint16",
	Uint:         "uint",
	Uint32:       "uint32",
	Uint64:       "uint64",
	Float32:      "float32",
	Float64:      "float64",
	Bool:         "bool",
	StrSlice:     "str_slice",
	Int8Slice:    "int8_slice",
	Int16Slice:   "int16_slice",
	IntSlice:     "int_slice",
	Int32Slice:   "int32_slice",
	Int64Slice:   "int64_slice",
	Float32Slice: "float32_slice",
	Float64Slice: "float64_slice",
	BoolSlice:    "bool_slice",
}

// testItem testItem Tableの構造体
type testItem struct {
	Item         `dynamodbav:"-"`
	HashKey      string    `dynamodbav:"hash_key"`
	GSIHashKey   string    `dynamodbav:"gsi_hash_key"`
	GSIRangeKey  string    `dynamodbav:"gsi_range_key"`
	FilterKey    string    `dynamodbav:"filter_key"`
	Str          string    `dynamodbav:"str"`
	Int8         int8      `dynamodbav:"int8"`
	Int16        int16     `dynamodbav:"int16"`
	Int          int       `dynamodbav:"int"`
	Int32        int32     `dynamodbav:"int32"`
	Int64        int64     `dynamodbav:"int64"`
	Uint8        uint8     `dynamodbav:"uint8"`
	Uint16       uint16    `dynamodbav:"uint16"`
	Uint         uint      `dynamodbav:"uint"`
	Uint32       uint32    `dynamodbav:"uint32"`
	Uint64       uint64    `dynamodbav:"uint64"`
	Float32      float32   `dynamodbav:"float32"`
	Float64      float64   `dynamodbav:"float64"`
	Bool         bool      `dynamodbav:"bool"`
	StrSlice     []string  `dynamodbav:"str_slice"`
	Int8Slice    []int8    `dynamodbav:"int8_slice"`
	Int16Slice   []int16   `dynamodbav:"int16_slice"`
	IntSlice     []int     `dynamodbav:"int_slice"`
	Int32Slice   []int32   `dynamodbav:"int32_slice"`
	Int64Slice   []int64   `dynamodbav:"int64_slice"`
	Float32Slice []float32 `dynamodbav:"float32_slice"`
	Float64Slice []float64 `dynamodbav:"float64_slice"`
	BoolSlice    []bool    `dynamodbav:"bool_slice"`
}

// testItemPrimaryIndex testItemテーブルのPrimaryIndex
type testItemPrimaryIndex struct {
	PrimaryIndex `dynamodbav:"-"`
	HashKey      string `dynamodbav:"hash_key"`
}

// testItemGSI testItemテーブルのGSI
// nolint
type testItemGSI struct {
	GlobalSecondaryIndex `dynamodbav:"-"`
	GSIHashKey           string `dynamodbav:"gsi_hash_key"`
	GSIRangeKey          string `dynamodbav:"gsi_range_key"`
}

func (e testItem) TableName() string {
	return testItemTableName
}
