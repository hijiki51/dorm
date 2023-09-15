package dorm

import (
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/google/go-cmp/cmp"
)

func cmpExprProj(x, y expression.ProjectionBuilder) bool {
	xb, err := expression.NewBuilder().WithProjection(x).Build()
	if err != nil {
		return false
	}
	yb, err := expression.NewBuilder().WithProjection(y).Build()
	if err != nil {
		return false
	}
	sameProj := *(xb.Projection()) == *(yb.Projection())
	diff := cmp.Diff(xb.Names(), yb.Names())

	return sameProj && diff == ""
}

func cmpUpdateExpr(x, y expression.UpdateBuilder) bool {
	xb, err := expression.NewBuilder().WithUpdate(x).Build()
	if err != nil {
		return false
	}
	yb, err := expression.NewBuilder().WithUpdate(y).Build()
	if err != nil {
		return false
	}
	sameUpdate := *(xb.Update()) == *(yb.Update())
	sameExpr := reflect.DeepEqual(xb.Values(), yb.Values())
	return sameUpdate && sameExpr
}

func TestProjectionAll(t *testing.T) {
	t.Run("success", testProjectionAllSuccess)
	t.Run("success with untaged fields", testProjectionAllSuccessUntagged)
	t.Run("success with not dynamodbav fields", testProjectionAllSuccessNotDDBAV)
}

func TestAttributeSetAll(t *testing.T) {
	t.Run("success", testAttributeSetAllSuccess)
	t.Run("success with untaged fields", testAttributeSetAllSuccessUntagged)
	t.Run("success with not dynamodbav fields", testAttributeSetAllSuccessNotDDBAV)
}

func testProjectionAllSuccess(t *testing.T) {
	t.Parallel()

	type args struct {
		optFns []func(name string) bool
	}

	testarg := args{
		optFns: []func(string) bool{},
	}

	names := []expression.NameBuilder{
		expression.Name("string"),
		expression.Name("bool"),
		expression.Name("int"),
	}

	want := expression.NamesList(names[0], names[1:]...)
	opts := []cmp.Option{
		cmp.Comparer(cmpExprProj),
	}

	wantErr := false

	if diff := cmp.Diff(want, ProjectionAll[utilTestItem](testarg.optFns...), opts...); len(diff) > 0 && !wantErr {
		t.Errorf("Compare value is mismatch (-want +got):%s\n", diff)
	}

}
func testProjectionAllSuccessUntagged(t *testing.T) {
	t.Parallel()

	type args struct {
		optFns []func(name string) bool
	}

	testarg := args{
		optFns: []func(string) bool{},
	}

	names := []expression.NameBuilder{
		expression.Name("string"),
		expression.Name("bool"),
		expression.Name("int"),
	}

	want := expression.NamesList(names[0], names[1:]...)
	opts := []cmp.Option{
		cmp.Comparer(cmpExprProj),
	}

	wantErr := false

	if diff := cmp.Diff(want, ProjectionAll[utilTestItemUntagged](testarg.optFns...), opts...); len(diff) > 0 && !wantErr {
		t.Errorf("Compare value is mismatch (-want +got):%s\n", diff)
	}

}

func testProjectionAllSuccessNotDDBAV(t *testing.T) {
	t.Parallel()

	type args struct {
		optFns []func(name string) bool
	}

	testarg := args{
		optFns: []func(string) bool{},
	}

	names := []expression.NameBuilder{
		expression.Name("string"),
		expression.Name("bool"),
		expression.Name("int"),
	}
	want := expression.NamesList(names[0], names[1:]...)
	opts := []cmp.Option{
		cmp.Comparer(cmpExprProj),
	}

	wantErr := false

	if diff := cmp.Diff(want, ProjectionAll[utilTestItemNotDynamodbav](testarg.optFns...), opts...); len(diff) > 0 && !wantErr {
		t.Errorf("Compare value is mismatch (-want +got):%s\n", diff)
	}

}

func testAttributeSetAllSuccess(t *testing.T) {
	t.Parallel()

	type args struct {
		str    utilTestItem
		optFns []func(name string) bool
	}

	testarg := args{
		str: utilTestItem{
			String: "string",
			Bool:   true,
			Int:    1,
		},
		optFns: []func(string) bool{},
	}

	want := expression.UpdateBuilder{}.Set(expression.Name("string"), expression.Value("string")).
		Set(expression.Name("bool"), expression.Value(true)).
		Set(expression.Name("int"), expression.Value(1))
	opts := []cmp.Option{
		cmp.Comparer(cmpUpdateExpr),
	}

	wantErr := false

	if diff := cmp.Diff(want, AttributeSetAll(testarg.str, testarg.optFns...), opts...); len(diff) > 0 && !wantErr {
		t.Errorf("Compare value is mismatch (-want +got):%s\n", diff)
	}

}
func testAttributeSetAllSuccessUntagged(t *testing.T) {
	t.Parallel()

	type args struct {
		str    utilTestItemUntagged
		optFns []func(name string) bool
	}

	testarg := args{
		str: utilTestItemUntagged{
			String:   "string",
			Bool:     true,
			Int:      1,
			Untagged: "untagged",
		},
		optFns: []func(string) bool{},
	}

	want := expression.UpdateBuilder{}.Set(expression.Name("string"), expression.Value("string")).
		Set(expression.Name("bool"), expression.Value(true)).
		Set(expression.Name("int"), expression.Value(1))
	opts := []cmp.Option{
		cmp.Comparer(cmpUpdateExpr),
	}

	wantErr := false

	if diff := cmp.Diff(want, AttributeSetAll(testarg.str, testarg.optFns...), opts...); len(diff) > 0 && !wantErr {
		t.Errorf("Compare value is mismatch (-want +got):%s\n", diff)
	}

}

func testAttributeSetAllSuccessNotDDBAV(t *testing.T) {
	t.Parallel()

	type args struct {
		str    utilTestItemNotDynamodbav
		optFns []func(name string) bool
	}

	testarg := args{
		str: utilTestItemNotDynamodbav{
			String:    "string",
			Bool:      true,
			Int:       1,
			NotTagged: "not_tagged",
		},
		optFns: []func(string) bool{},
	}

	want := expression.UpdateBuilder{}.Set(expression.Name("string"), expression.Value("string")).
		Set(expression.Name("bool"), expression.Value(true)).
		Set(expression.Name("int"), expression.Value(1))
	opts := []cmp.Option{
		cmp.Comparer(cmpUpdateExpr),
	}

	wantErr := false

	if diff := cmp.Diff(want, AttributeSetAll(testarg.str, testarg.optFns...), opts...); len(diff) > 0 && !wantErr {
		t.Errorf("Compare value is mismatch (-want +got):%s\n", diff)
	}
}

type utilTestItem struct {
	Item
	String string `dynamodbav:"string"`
	Bool   bool   `dynamodbav:"bool"`
	Int    int    `dynamodbav:"int"`
}

type utilTestItemUntagged struct {
	Item
	String   string `dynamodbav:"string"`
	Bool     bool   `dynamodbav:"bool"`
	Int      int    `dynamodbav:"int"`
	Untagged string
}

type utilTestItemNotDynamodbav struct {
	Item
	String    string `dynamodbav:"string"`
	Bool      bool   `dynamodbav:"bool"`
	Int       int    `dynamodbav:"int"`
	NotTagged string `json:"not_tagged"`
}

func (i utilTestItem) TableName() string { return "test" }
func (i utilTestItemUntagged) TableName() string {
	return "test"
}
func (i utilTestItemNotDynamodbav) TableName() string {
	return "test"
}
