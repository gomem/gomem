// Copyright 2019 Nick Poorman
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dataframe

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/gomem/gomem/pkg/iterator"
	"github.com/gomem/gomem/pkg/smartbuilder"
)

const (
	NUMROWS  = int64(30)
	NUMCOLS  = 2
	COL0NAME = "f1-i32"
	COL1NAME = "f2-f64"
)

func buildRecords(pool *memory.CheckedAllocator, t *testing.T, last int32) ([]array.Record, *arrow.Schema) {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: COL0NAME, Type: arrow.PrimitiveTypes.Int32},
			{Name: COL1NAME, Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	b.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3, 4, 5, 6}, nil)
	b.Field(0).(*array.Int32Builder).AppendValues([]int32{7, 8, 9, 10}, []bool{true, true, false, true})
	b.Field(1).(*array.Float64Builder).AppendValues([]float64{1, 2, 3, 4, 5, 6}, nil)
	b.Field(1).(*array.Float64Builder).AppendValues([]float64{7, 8, 9, 10}, []bool{true, true, false, true})

	rec1 := b.NewRecord()

	b.Field(0).(*array.Int32Builder).AppendValues([]int32{11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, nil)
	b.Field(1).(*array.Float64Builder).AppendValues([]float64{11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, nil)

	rec2 := b.NewRecord()

	b.Field(0).(*array.Int32Builder).AppendValues([]int32{31, 32, 33, 34, 35, 36, 37, 38, 39, last}, nil)
	b.Field(1).(*array.Float64Builder).AppendValues([]float64{31, 32, 33, 34, 35, 36, 37, 38, 39, 40}, nil)

	rec3 := b.NewRecord()

	return []array.Record{rec1, rec2, rec3}, schema
}

func getColumns(pool *memory.CheckedAllocator, t *testing.T, last int32) []array.Column {
	records, schema := buildRecords(pool, t, last)
	for i := range records {
		defer records[i].Release()
	}

	tbl := array.NewTableFromRecords(schema, records)
	defer tbl.Release()

	cols := make([]array.Column, tbl.NumCols())
	for i := range cols {
		col := tbl.Column(i)
		col.Retain()
		cols[i] = *col
	}

	return cols
}

func genValues(length int) []int32 {
	colVals := make([]int32, 30)
	for i := range colVals {
		colVals[i] = int32(i)
	}
	return colVals
}

func TestNewDataFrameFromColumns(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	cols := getColumns(pool, t, 40)
	for i := range cols {
		defer cols[i].Release()
	}

	df, err := NewDataFrameFromColumns(pool, cols)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	if got, want := df.NumRows(), NUMROWS; got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}
}

func TestNumCols(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	cols := getColumns(pool, t, 40)
	for i := range cols {
		defer cols[i].Release()
	}

	df, err := NewDataFrameFromColumns(pool, cols)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	if got, want := df.NumCols(), NUMCOLS; got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}
}

func TestNumRows(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	cols := getColumns(pool, t, 40)
	for i := range cols {
		defer cols[i].Release()
	}

	df, err := NewDataFrameFromColumns(pool, cols)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	if got, want := df.NumRows(), NUMROWS; got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}
}

func TestDims(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	cols := getColumns(pool, t, 40)
	for i := range cols {
		defer cols[i].Release()
	}

	df, err := NewDataFrameFromColumns(pool, cols)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	w, l := df.Dims()

	if got, want := w, NUMCOLS; got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	if got, want := l, NUMROWS; got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}
}

func TestEquals(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	cols := getColumns(pool, t, 40)
	for i := range cols {
		defer cols[i].Release()
	}

	df, err := NewDataFrameFromColumns(pool, cols)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	cols2 := getColumns(pool, t, 40)
	defer func() {
		for _, col := range cols2 {
			col.Release()
		}
	}()

	df2, err := NewDataFrameFromColumns(pool, cols2)
	if err != nil {
		t.Fatal(err)
	}
	defer df2.Release()

	if got, want := df.Equals(df2), true; got != want {
		t.Fatalf("got=%v, want=%v", got, want)
	}
}

func TestEqualsFalse(t *testing.T) {
	// This test makes sure Equals returns false as well as true.
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	cols := getColumns(pool, t, 40)
	for i := range cols {
		defer cols[i].Release()
	}

	df, err := NewDataFrameFromColumns(pool, cols)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	cols2 := getColumns(pool, t, 99)
	defer func() {
		for _, col := range cols2 {
			col.Release()
		}
	}()

	df2, err := NewDataFrameFromColumns(pool, cols2)
	if err != nil {
		t.Fatal(err)
	}
	defer df2.Release()

	if got, want := df.Equals(df2), false; got != want {
		t.Fatalf("got=%v, want=%v", got, want)
	}
}

func TestName(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	cols := getColumns(pool, t, 40)
	for i := range cols {
		defer cols[i].Release()
	}

	df, err := NewDataFrameFromColumns(pool, cols)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	if got, want := df.Name(0), COL0NAME; got != want {
		t.Fatalf("got=%s, want=%s", got, want)
	}

	if got, want := df.Name(1), COL1NAME; got != want {
		t.Fatalf("got=%s, want=%s", got, want)
	}
}

func TestSlice(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	cols := getColumns(pool, t, 40)
	for i := range cols {
		defer cols[i].Release()
	}

	df, err := NewDataFrameFromColumns(pool, cols)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	df2, err := df.Slice(0, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer df2.Release()

	if got, want := df2.NumRows(), int64(5); got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}
}

func TestColumnNames(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	df, err := NewDataFrameFromMem(pool, Dict{
		"col1-i32": []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		"col2-f64": []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	if got, want := df.ColumnNames(), []string{"col1-i32", "col2-f64"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v, want=%v", got, want)
	}
}

func TestColumnTypes(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	df, err := NewDataFrameFromMem(pool, Dict{
		"col1-i32": []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		"col2-f64": []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	field1 := arrow.Field{
		Name:     "col1-i32",
		Type:     arrow.PrimitiveTypes.Int32,
		Nullable: false,
		Metadata: arrow.Metadata{},
	}
	field2 := arrow.Field{
		Name:     "col2-f64",
		Type:     arrow.PrimitiveTypes.Float64,
		Nullable: false,
		Metadata: arrow.Metadata{},
	}

	if got, want := df.ColumnTypes(), []arrow.Field{field1, field2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v, want=%v", got, want)
	}
}

func TestAppendColumn(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	cols := getColumns(pool, t, 40)
	for i := range cols {
		defer cols[i].Release()
	}

	df, err := NewDataFrameFromColumns(pool, cols)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	// Create a new Column to append
	col, err := NewColumnFromMem(pool, "col3-i32", genValues(int(df.NumRows())))
	if err != nil {
		t.Fatal(err)
	}
	defer col.Release()

	largerDf, err := df.AppendColumn(col)
	if err != nil {
		t.Fatal(err)
	}
	defer largerDf.Release()

	got := largerDf.Display(-1)
	want := `rec[0]["f1-i32"]: [1 2 3 4 5 6 7 8 (null) 10]
rec[0]["f2-f64"]: [1 2 3 4 5 6 7 8 (null) 10]
rec[0]["col3-i32"]: [0 1 2 3 4 5 6 7 8 9]
rec[1]["f1-i32"]: [11 12 13 14 15 16 17 18 19 20]
rec[1]["f2-f64"]: [11 12 13 14 15 16 17 18 19 20]
rec[1]["col3-i32"]: [10 11 12 13 14 15 16 17 18 19]
rec[2]["f1-i32"]: [31 32 33 34 35 36 37 38 39 40]
rec[2]["f2-f64"]: [31 32 33 34 35 36 37 38 39 40]
rec[2]["col3-i32"]: [20 21 22 23 24 25 26 27 28 29]
`

	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func TestCopy(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	df, err := NewDataFrameFromMem(pool, Dict{
		"col1-i32": []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		"col2-f64": []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	df2, err := df.Copy()
	if err != nil {
		t.Fatal(err)
	}
	defer df2.Release()

	got := df2.Display(-1)
	want := `rec[0]["col1-i32"]: [1 2 3 4 5 6 7 8 9 10]
rec[0]["col2-f64"]: [1 2 3 4 5 6 7 8 9 10]
`
	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}

	if &df == &df2 {
		t.Fatalf("references are the same. df is not a copy of df2 (%v) == (%v)", &df, &df2)
	}
}

func TestSelect(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	df, err := NewDataFrameFromMem(pool, Dict{
		"col1-i32": []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		"col2-f64": []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		"col3-i32": []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		"col4-f64": []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		"col5-i32": []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		"col6-f64": []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	names := []string{"col1-i32", "col3-i32", "col6-f64"}
	df2, err := df.Select(names...)
	if err != nil {
		t.Fatal(err)
	}
	defer df2.Release()

	got := df2.Display(-1)
	want := `rec[0]["col1-i32"]: [1 2 3 4 5 6 7 8 9 10]
rec[0]["col3-i32"]: [1 2 3 4 5 6 7 8 9 10]
rec[0]["col6-f64"]: [1 2 3 4 5 6 7 8 9 10]
`

	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func TestDrop(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	df, err := NewDataFrameFromMem(pool, Dict{
		"col1-i32": []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		"col2-f64": []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		"col3-i32": []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		"col4-f64": []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		"col5-i32": []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		"col6-f64": []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	names := []string{"col1-i32", "col3-i32", "col6-f64"}
	df2, err := df.Drop(names...)
	if err != nil {
		t.Fatal(err)
	}
	defer df2.Release()

	got := df2.Display(-1)
	want := `rec[0]["col2-f64"]: [1 2 3 4 5 6 7 8 9 10]
rec[0]["col4-f64"]: [1 2 3 4 5 6 7 8 9 10]
rec[0]["col5-i32"]: [1 2 3 4 5 6 7 8 9 10]
`

	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func TestNewDataFrameFromMem(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	df, err := NewDataFrameFromMem(pool, Dict{
		"col1-i32": []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		"col2-f64": []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	got := df.Display(5)
	want := `rec[0]["col1-i32"]: [1 2 3 4 5]
rec[0]["col2-f64"]: [1 2 3 4 5]
rec[1]["col1-i32"]: [6 7 8 9 10]
rec[1]["col2-f64"]: [6 7 8 9 10]
`
	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func TestNewColumnFromSparseMem(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	values := []interface{}{1, nil, 3}
	valueIndexes := []int{0, 2, 4}
	col, err := NewColumnFromSparseMem(pool, "sparse-col-i32", values, valueIndexes, 10)
	if err != nil {
		t.Fatal(err)
	}
	defer col.Release()

	df, err := NewDataFrameFromColumns(pool, []array.Column{*col})
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	got := df.Display(-1)
	want := `rec[0]["sparse-col-i32"]: [1 (null) 0 (null) 3 (null) (null) (null) (null) (null)]
`
	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func TestColumn(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	df, err := NewDataFrameFromMem(pool, Dict{
		"col1-i32": []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		"col2-f64": []float64{10, 12, 13, 14, 15, 16, 17, 18, 19, 20},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	name := "col2-f64"

	col := df.Column(name)
	if col == nil {
		t.Fatal("col should not be nil")
	}

	// Column should have the same name
	if got, want := col.Name(), name; got != want {
		t.Fatalf("got=%v, want=%v", got, want)
	}

	// Pointer should be the same
	cols := df.Columns()
	if got, want := &cols[1], col; got != want {
		t.Fatalf("got=%v, want=%v", got, want)
	}
}

func TestColumnAt(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	df, err := NewDataFrameFromMem(pool, Dict{
		"col1-i32": []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		"col2-f64": []float64{10, 12, 13, 14, 15, 16, 17, 18, 19, 20},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	col := df.ColumnAt(1)
	if col == nil {
		t.Fatal("col should not be nil")
	}

	// Pointer should be the same
	cols := df.Columns()
	if got, want := &cols[1], col; got != want {
		t.Fatalf("got=%v, want=%v", got, want)
	}
}

func TestLeftJoin(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	leftDf, err := NewDataFrameFromMem(pool, Dict{
		"A": []float32{5, 2, 3, 1},
		"B": []float64{6, 4, 3, 2},
		"C": []float64{1.7, 2.3, 2.3, 7.8},
		"D": []float64{5, 1, 0, 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer leftDf.Release()

	rightDf, err := NewDataFrameFromMem(pool, Dict{
		"A": []float32{5, 4, 2, 5},
		"F": []float64{7, 3, 5, 8},
		"D": []float64{5, 0, 0, 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer rightDf.Release()

	joinedDf, err := leftDf.LeftJoin(rightDf, []string{"A", "D"})
	if err != nil {
		t.Fatal(err)
	}
	defer joinedDf.Release()

	got := joinedDf.Display(-1)
	want := `rec[0]["A"]: [5 2 3 1]
rec[0]["D"]: [5 1 0 0]
rec[0]["B"]: [6 4 3 2]
rec[0]["C"]: [1.7 2.3 2.3 7.8]
rec[0]["F"]: [7 (null) (null) (null)]
`
	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func TestLeftJoinCase2(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	// This test is meant to test LeftJoin
	// when there will be duplicate leftDf rows
	// because they matched more than one rightDf row.
	leftDf, err := NewDataFrameFromMem(pool, Dict{
		"A": []float64{5, 2, 3, 1},
		"B": []float64{6, 4, 3, 2},
		"C": []float64{1.7, 2.3, 2.3, 7.8},
		"D": []float64{5, 1, 0, 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer leftDf.Release()

	rightDf, err := NewDataFrameFromMem(pool, Dict{
		"A": []float64{5, 4, 2, 5, 3, 3},
		"F": []float64{7, 3, 5, 8, 99, 44},
		"D": []float64{5, 0, 0, 0, 0, 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer rightDf.Release()

	joinedDf, err := leftDf.LeftJoin(rightDf, []string{"A", "D"})
	if err != nil {
		t.Fatal(err)
	}
	defer joinedDf.Release()

	got := joinedDf.Display(-1)
	want := `rec[0]["A"]: [5 2 3 3 1]
rec[0]["D"]: [5 1 0 0 0]
rec[0]["B"]: [6 4 3 3 2]
rec[0]["C"]: [1.7 2.3 2.3 2.3 7.8]
rec[0]["F"]: [7 (null) 99 44 (null)]
`
	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func TestLeftJoinCase3(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	// This test is meant to test LeftJoin
	// when there is only one column to match on
	// that would result in duplicate columns.
	leftDf, err := NewDataFrameFromMem(pool, Dict{
		"A": []float64{5, 2, 3, 1},
		"B": []float64{6, 4, 3, 2},
		"C": []float64{1.7, 2.3, 2.3, 7.8},
		"D": []float64{5, 1, 0, 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer leftDf.Release()

	rightDf, err := NewDataFrameFromMem(pool, Dict{
		"A": []float64{5, 4, 2, 5, 3, 3},
		"F": []float64{7, 3, 5, 8, 99, 44},
		"D": []float64{5, 0, 0, 0, 0, 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer rightDf.Release()

	joinedDf, err := leftDf.LeftJoin(rightDf, []string{"A"})
	if err != nil {
		t.Fatal(err)
	}
	defer joinedDf.Release()

	got := joinedDf.Display(-1)
	want := `rec[0]["A"]: [5 5 2 3 3 1]
rec[0]["B"]: [6 6 4 3 3 2]
rec[0]["C"]: [1.7 1.7 2.3 2.3 2.3 7.8]
rec[0]["D_0"]: [5 5 1 0 0 0]
rec[0]["D_1"]: [5 0 0 0 0 (null)]
rec[0]["F"]: [7 8 5 99 44 (null)]
`
	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func TestRightJoin(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	leftDf, err := NewDataFrameFromMem(pool, Dict{
		"A": []float64{5, 2, 3, 1},
		"B": []float64{6, 4, 3, 2},
		"C": []float64{1.7, 2.3, 2.3, 7.8},
		"D": []float64{5, 1, 0, 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer leftDf.Release()

	rightDf, err := NewDataFrameFromMem(pool, Dict{
		"A": []float64{5, 4, 2, 5},
		"F": []float64{7, 3, 5, 8},
		"D": []float64{5, 0, 0, 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer rightDf.Release()

	joinedDf, err := leftDf.RightJoin(rightDf, []string{"A", "D"})
	if err != nil {
		t.Fatal(err)
	}
	defer joinedDf.Release()

	got := joinedDf.Display(-1)
	want := `rec[0]["A"]: [5 4 2 5]
rec[0]["D"]: [5 0 0 0]
rec[0]["F"]: [7 3 5 8]
rec[0]["B"]: [6 (null) (null) (null)]
rec[0]["C"]: [1.7 (null) (null) (null)]
`
	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func TestRightJoinCase2(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	// This test is meant to test RightJoin
	// when there will be duplicate rightDf rows
	// because they matched more than one leftDf row.
	leftDf, err := NewDataFrameFromMem(pool, Dict{
		"A": []float64{5, 2, 5, 1, 4},
		"B": []float64{6, 4, 3, 2, 9},
		"C": []float64{1.7, 2.3, 2.3, 7.8, 9.1},
		"D": []float64{5, 1, 5, 0, 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer leftDf.Release()

	rightDf, err := NewDataFrameFromMem(pool, Dict{
		"A": []float64{5, 4, 8},
		"F": []float64{7, 3, 8},
		"D": []float64{5, 0, 8},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer rightDf.Release()

	joinedDf, err := leftDf.RightJoin(rightDf, []string{"A", "D"})
	if err != nil {
		t.Fatal(err)
	}
	defer joinedDf.Release()

	got := joinedDf.Display(-1)
	want := `rec[0]["A"]: [5 5 4 8]
rec[0]["D"]: [5 5 0 8]
rec[0]["F"]: [7 7 3 8]
rec[0]["B"]: [6 3 9 (null)]
rec[0]["C"]: [1.7 2.3 9.1 (null)]
`
	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func TestRightJoinCase3(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	// This test is meant to test RightJoin
	// when there is only one column to match on
	// that would result in duplicate columns.
	leftDf, err := NewDataFrameFromMem(pool, Dict{
		"A": []float64{5, 2, 3, 1},
		"B": []float64{6, 4, 3, 2},
		"C": []float64{1.7, 2.3, 2.3, 7.8},
		"D": []float64{5, 1, 0, 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer leftDf.Release()

	rightDf, err := NewDataFrameFromMem(pool, Dict{
		"A": []float64{5, 4, 2, 5, 3, 3},
		"F": []float64{7, 3, 5, 8, 99, 44},
		"D": []float64{5, 0, 0, 0, 0, 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer rightDf.Release()

	joinedDf, err := leftDf.RightJoin(rightDf, []string{"A"})
	if err != nil {
		t.Fatal(err)
	}
	defer joinedDf.Release()

	got := joinedDf.Display(-1)
	want := `rec[0]["A"]: [5 4 2 5 3 3]
rec[0]["D_1"]: [5 0 0 0 0 0]
rec[0]["F"]: [7 3 5 8 99 44]
rec[0]["B"]: [6 (null) 4 6 3 3]
rec[0]["C"]: [1.7 (null) 2.3 1.7 2.3 2.3]
rec[0]["D_0"]: [5 (null) 1 5 0 0]
`
	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func TestInnerJoinCase1(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	leftDf, err := NewDataFrameFromMem(pool, Dict{
		"A": []float64{1, 7, 6, 1},
		"B": []float64{2.1, 2.2, 2.3, 2.4},
		"C": []float64{3.3, 8.0, 8.0, 1.1},
		"D": []float64{5, 3, 2, 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer leftDf.Release()

	rightDf, err := NewDataFrameFromMem(pool, Dict{
		"A": []float64{2, 0, 6, 1, 6},
		"F": []float64{2, 5, 2, 8, 9},
		"D": []float64{2, 7, 2, 2, 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer rightDf.Release()

	joinedDf, err := leftDf.InnerJoin(rightDf, []string{"A", "D"})
	if err != nil {
		t.Fatal(err)
	}
	defer joinedDf.Release()

	got := joinedDf.Display(-1)
	want := `rec[0]["A"]: [6 6]
rec[0]["D"]: [2 2]
rec[0]["B"]: [2.3 2.3]
rec[0]["C"]: [8 8]
rec[0]["F"]: [2 9]
`
	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func TestOuterJoin(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	leftDf, err := NewDataFrameFromMem(pool, Dict{
		"A": []int32{5, 2, 3, 1},
		"B": []float64{6, 4, 3, 2},
		"C": []float64{1.7, 2.3, 2.3, 7.8},
		"D": []int64{5, 1, 0, 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer leftDf.Release()

	rightDf, err := NewDataFrameFromMem(pool, Dict{
		"A": []int32{5, 4, 2, 5},
		"F": []float64{7, 3, 5, 8},
		"D": []int64{5, 0, 0, 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer rightDf.Release()

	joinedDf, err := leftDf.OuterJoin(rightDf, []string{"A", "D"})
	if err != nil {
		t.Fatal(err)
	}
	defer joinedDf.Release()

	got := joinedDf.Display(-1)
	want := `rec[0]["A"]: [5 2 3 1 4 2 5]
rec[0]["D"]: [5 1 0 0 0 0 0]
rec[0]["B"]: [6 4 3 2 (null) (null) (null)]
rec[0]["C"]: [1.7 2.3 2.3 7.8 (null) (null) (null)]
rec[0]["F"]: [7 (null) (null) (null) 3 5 8]
`
	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func TestOuterJoinCase2(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	leftDf, err := NewDataFrameFromMem(pool, Dict{
		"A": []uint8{5, 2, 3, 1},
		"B": []float64{6, 4, 3, 2},
		"C": []float64{1.7, 2.3, 2.3, 7.8},
		"D": []int16{5, 1, 0, 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer leftDf.Release()

	rightDf, err := NewDataFrameFromMem(pool, Dict{
		"A": []uint8{5, 4, 2, 5},
		"F": []int8{7, 3, 5, 8},
		"D": []int16{5, 0, 0, 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer rightDf.Release()

	joinedDf, err := leftDf.OuterJoin(rightDf, []string{"A"})
	if err != nil {
		t.Fatal(err)
	}
	defer joinedDf.Release()

	got := joinedDf.Display(-1)
	want := `rec[0]["A"]: [5 5 2 3 1 4]
rec[0]["B"]: [6 6 4 3 2 (null)]
rec[0]["C"]: [1.7 1.7 2.3 2.3 7.8 (null)]
rec[0]["D_0"]: [5 5 1 0 0 (null)]
rec[0]["D_1"]: [5 0 0 (null) (null) 0]
rec[0]["F"]: [7 8 5 (null) (null) 3]
`
	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func TestOuterJoinCase3(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	// When elements are nil at the same location we should not consider them equal as they are unknown.
	// This follows SQL practices.
	leftDf, err := NewDataFrameFromMem(pool, Dict{
		"A": []interface{}{nil, 2, 3, 1},
		"B": []float64{6, 4, 3, 2},
		"C": []float64{1.7, 2.3, 2.3, 7.8},
		"D": []int64{5, 1, 0, 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer leftDf.Release()

	rightDf, err := NewDataFrameFromMem(pool, Dict{
		"A": []interface{}{nil, 4, 2, 5},
		"F": []float64{7, 3, 5, 8},
		"D": []int64{5, 0, 0, 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer rightDf.Release()

	joinedDf, err := leftDf.OuterJoin(rightDf, []string{"A", "D"})
	if err != nil {
		t.Fatal(err)
	}
	defer joinedDf.Release()

	got := joinedDf.Display(-1)
	want := `rec[0]["A"]: [(null) 2 3 1 (null) 4 2 5]
rec[0]["D"]: [5 1 0 0 5 0 0 0]
rec[0]["B"]: [6 4 3 2 (null) (null) (null) (null)]
rec[0]["C"]: [1.7 2.3 2.3 7.8 (null) (null) (null) (null)]
rec[0]["F"]: [(null) (null) (null) (null) 7 3 5 8]
`
	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func TestCrossJoin(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	leftDf, err := NewDataFrameFromMem(pool, Dict{
		"A": []int64{5, 2, 3, 1},
		"B": []float64{6, 4, 3, 2},
		"C": []float64{1.7, 2.3, 2.3, 7.8},
		"D": []float32{5, 1, 0, 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer leftDf.Release()

	rightDf, err := NewDataFrameFromMem(pool, Dict{
		"A": []int64{5, 4, 2, 5, 10},
		"F": []int32{7, 3, 5, 8, 11},
		"D": []float32{5, 0, 0, 0, 12},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer rightDf.Release()

	joinedDf, err := leftDf.CrossJoin(rightDf)
	if err != nil {
		t.Fatal(err)
	}
	defer joinedDf.Release()

	got := joinedDf.Display(-1)
	want := `rec[0]["A_0"]: [5 5 5 5 5 2 2 2 2 2 3 3 3 3 3 1 1 1 1 1]
rec[0]["B"]: [6 6 6 6 6 4 4 4 4 4 3 3 3 3 3 2 2 2 2 2]
rec[0]["C"]: [1.7 1.7 1.7 1.7 1.7 2.3 2.3 2.3 2.3 2.3 2.3 2.3 2.3 2.3 2.3 7.8 7.8 7.8 7.8 7.8]
rec[0]["D_0"]: [5 5 5 5 5 1 1 1 1 1 0 0 0 0 0 0 0 0 0 0]
rec[0]["A_1"]: [5 4 2 5 10 5 4 2 5 10 5 4 2 5 10 5 4 2 5 10]
rec[0]["D_1"]: [5 0 0 0 12 5 0 0 0 12 5 0 0 0 12 5 0 0 0 12]
rec[0]["F"]: [7 3 5 8 11 7 3 5 8 11 7 3 5 8 11 7 3 5 8 11]
`
	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func TestJoinSuffix(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	// This test is meant to test RightJoin
	// when there is only one column to match on
	// that would result in duplicate columns.
	leftDf, err := NewDataFrameFromMem(pool, Dict{
		"A": []float64{5, 2, 3, 1},
		"B": []float64{6, 4, 3, 2},
		"C": []float64{1.7, 2.3, 2.3, 7.8},
		"D": []float64{5, 1, 0, 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer leftDf.Release()

	rightDf, err := NewDataFrameFromMem(pool, Dict{
		"A": []float64{5, 4, 2, 5, 3, 3},
		"F": []float64{7, 3, 5, 8, 99, 44},
		"D": []float64{5, 0, 0, 0, 0, 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer rightDf.Release()

	joinedDf, err := leftDf.RightJoin(rightDf, []string{"A"}, WithLsuffix("_left"), WithRsuffix("_right"))
	if err != nil {
		t.Fatal(err)
	}
	defer joinedDf.Release()

	got := joinedDf.Display(-1)
	want := `rec[0]["A"]: [5 4 2 5 3 3]
rec[0]["D_right"]: [5 0 0 0 0 0]
rec[0]["F"]: [7 3 5 8 99 44]
rec[0]["B"]: [6 (null) 4 6 3 3]
rec[0]["C"]: [1.7 (null) 2.3 1.7 2.3 2.3]
rec[0]["D_left"]: [5 (null) 1 5 0 0]
`
	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func TestInconsistentDataTypesError(t *testing.T) {
	// When elements are nil at the same location we should not consider them equal as they are unknown.
	// This follows SQL practices.
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	df, err := NewDataFrameFromMem(pool, Dict{
		"A": []interface{}{nil, 2, 3, 1.2},
		"B": []float64{6, 4, 3, 2},
		"C": []float64{1.7, 2.3, 2.3, 7.8},
		"D": []int64{5, 1, 0, 0},
	})
	if err == nil {
		defer df.Release()
	}

	var v int
	got := err
	want := fmt.Errorf("inconsistent data types for elements, expecting %v to be of type (%T)", 1.2, v)
	if got.Error() != want.Error() {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

// multByN takes a DataFrame and multiplies a the column by the provided multipier.
func multByN(columnName string, multipier float64) MutationFunc {
	return func(df *DataFrame) (*DataFrame, error) {
		col := df.Column(columnName)
		schema := arrow.NewSchema([]arrow.Field{col.Field()}, nil)
		builder := array.NewRecordBuilder(df.Allocator(), schema)
		defer builder.Release()
		smartBuilder := smartbuilder.NewSmartBuilder(builder)
		valueIterator := iterator.NewFloat64ValueIterator(col)
		defer valueIterator.Release()
		for valueIterator.Next() {
			value, isNil := valueIterator.Value()
			if isNil {
				smartBuilder.Append(0, nil)
			} else {
				value *= multipier
				smartBuilder.Append(0, value)
			}
		}
		rec := builder.NewRecord()
		defer rec.Release()
		chunk := array.NewChunked(col.DataType(), rec.Columns())
		defer chunk.Release()
		newCol := array.NewColumn(col.Field(), chunk)
		defer newCol.Release()
		df2, err := df.Drop(columnName)
		if err != nil {
			return nil, err
		}
		defer df2.Release()
		return df2.AppendColumn(newCol)
	}
}

func TestApply(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	df, err := NewDataFrameFromMem(pool, Dict{
		"col1-i32": []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		"col2-f64": []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	df2, err := df.Apply(multByN("col2-f64", 2.0), multByN("col2-f64", -1.0))
	if err != nil {
		t.Fatal(err)
	}
	defer df2.Release()

	got := df2.Display(-1)
	want := `rec[0]["col1-i32"]: [1 2 3 4 5 6 7 8 9 10]
rec[0]["col2-f64"]: [-2 -4 -6 -8 -10 -12 -14 -16 -18 -20]
`
	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}

	if &df == &df2 {
		t.Fatalf("references are the same. df is not a copy of df2 (%v) == (%v)", &df, &df2)
	}
}

func TestApplyToColumn(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	df, err := NewDataFrameFromMem(pool, Dict{
		"col1-i32": []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		"col2-f64": []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	df2, err := df.ApplyToColumn("col2-f64", "col2-f64-x2", func(v interface{}) (interface{}, error) {
		// This function will be called for every element in "col2-f64"
		if v == nil {
			// can't multiply nil by anything
			return nil, nil
		}
		value, ok := v.(float64)
		if !ok {
			return nil, errors.New("v is not a float64")
		}
		value *= 2
		return value, nil
	})

	if err != nil {
		t.Fatal(err)
	}
	defer df2.Release()

	got := df2.Display(-1)
	want := `rec[0]["col1-i32"]: [1 2 3 4 5 6 7 8 9 10]
rec[0]["col2-f64"]: [1 2 3 4 5 6 7 8 9 10]
rec[0]["col2-f64-x2"]: [2 4 6 8 10 12 14 16 18 20]
`
	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}

	if &df == &df2 {
		t.Fatalf("references are the same. df is not a copy of df2 (%v) == (%v)", &df, &df2)
	}
}

func TestNewDataFrameFromTable(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	records, schema := buildRecords(pool, t, 48)
	for i := range records {
		defer records[i].Release()
	}

	table := array.NewTableFromRecords(schema, records)
	defer table.Release()

	df, err := NewDataFrameFromTable(pool, table)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	got := df.Display(-1)
	want := `rec[0]["f1-i32"]: [1 2 3 4 5 6 7 8 (null) 10]
rec[0]["f2-f64"]: [1 2 3 4 5 6 7 8 (null) 10]
rec[1]["f1-i32"]: [11 12 13 14 15 16 17 18 19 20]
rec[1]["f2-f64"]: [11 12 13 14 15 16 17 18 19 20]
rec[2]["f1-i32"]: [31 32 33 34 35 36 37 38 39 48]
rec[2]["f2-f64"]: [31 32 33 34 35 36 37 38 39 40]
`
	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}
