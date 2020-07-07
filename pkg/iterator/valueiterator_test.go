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

package iterator_test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/float16"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/gomem/gomem/pkg/iterator"
)

func TestInt32ValueIterator(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	records, schema := buildRecords(pool, t)
	var numRows int64
	for i := range records {
		defer records[i].Release()
		numRows += records[i].NumRows()
	}

	expectedValues := make([]int32, 0, numRows)
	expectedValuesBool := make([]bool, 0, numRows)
	for i := range records {
		ref := records[i].Column(0).(*array.Int32)
		values := ref.Int32Values()
		for j := range values {
			expectedValues = append(expectedValues, values[j])
			expectedValuesBool = append(expectedValuesBool, ref.IsNull(j))
		}
	}

	tbl := array.NewTableFromRecords(schema, records)
	defer tbl.Release()

	column := tbl.Column(0)
	cr := iterator.NewInt32ValueIterator(column)
	defer cr.Release()

	n := 0
	for cr.Next() {
		value, null := cr.Value()
		if got, want := value, expectedValues[n]; got != want {
			t.Fatalf("got=%d, want=%d", got, want)
		}
		if got, want := null, expectedValuesBool[n]; got != want {
			t.Fatalf("got=%v, want=%v", got, want)
		}
		n++
	}
}

func TestInt32ValueIteratorPointer(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	records, schema := buildRecords(pool, t)
	var numRows int64
	for i := range records {
		defer records[i].Release()
		numRows += records[i].NumRows()
	}

	expectedPtrs := make([]*int32, 0, numRows)
	for i := range records {
		ref := records[i].Column(0).(*array.Int32)
		values := ref.Int32Values()
		for j := range values {
			if ref.IsNull(j) {
				expectedPtrs = append(expectedPtrs, nil)
			} else {
				expectedPtrs = append(expectedPtrs, &values[j])
			}
		}
	}

	tbl := array.NewTableFromRecords(schema, records)
	defer tbl.Release()

	column := tbl.Column(0)
	cr := iterator.NewInt32ValueIterator(column)
	defer cr.Release()

	n := 0
	for cr.Next() {
		value := cr.ValuePointer()
		if got, want := value, expectedPtrs[n]; got != want {
			t.Fatalf("got=%d, want=%d", got, want)
		}
		n++
	}
}

func TestFloat64ValueIterator(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "f2-f64", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	expectedValues := []float64{
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
		11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
		31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
	}

	expectedValuesBool := []bool{
		true, true, true, true, true, true, true, true, true, true,
		true, false, true, false, true, true, true, true, true, false,
		true, true, true, true, true, true, true, true, true, true,
	}

	b.Field(0).(*array.Float64Builder).AppendValues(expectedValues[0:10], nil)
	rec1 := b.NewRecord()
	defer rec1.Release()

	b.Field(0).(*array.Float64Builder).AppendValues(expectedValues[10:20], expectedValuesBool[10:20])
	rec2 := b.NewRecord()
	defer rec2.Release()

	b.Field(0).(*array.Float64Builder).AppendValues(expectedValues[20:30], nil)
	rec3 := b.NewRecord()
	defer rec3.Release()

	records := []array.Record{rec1, rec2, rec3}
	tbl := array.NewTableFromRecords(schema, records)
	defer tbl.Release()
	column := tbl.Column(0)
	vr := iterator.NewFloat64ValueIterator(column)
	defer vr.Release()

	n := 0
	for vr.Next() {
		value, null := vr.Value()
		if got, want := value, expectedValues[n]; got != want {
			t.Fatalf("got=%f, want=%f", got, want)
		}
		if got, want := !null, expectedValuesBool[n]; got != want {
			t.Fatalf("got=%v, want=%v (n=%d)", got, want, n)
		}
		n++
	}
}

func TestDate32ValueIterator(t *testing.T) {
	t.Skip("TODO: Implement.")
}

func TestDate64ValueIterator(t *testing.T) {
	t.Skip("TODO: Implement.")
}

func TestBooleanValueIterator(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "c1-bools", Type: arrow.FixedWidthTypes.Boolean},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	expectedValues := []bool{
		true, true, true, true, true, true, true, true, true, true,
		false, false, false, false, false, false, false, false, false, false,
		true, true, false, true, true, true, true, true, true, true,
	}

	expectedValuesBool := []bool{
		true, true, true, true, true, true, true, true, true, true,
		true, false, true, false, true, true, true, true, true, false,
		true, true, true, true, true, true, true, true, true, true,
	}

	b.Field(0).(*array.BooleanBuilder).AppendValues(expectedValues[0:10], nil)
	rec1 := b.NewRecord()
	defer rec1.Release()

	b.Field(0).(*array.BooleanBuilder).AppendValues(expectedValues[10:20], expectedValuesBool[10:20])
	rec2 := b.NewRecord()
	defer rec2.Release()

	b.Field(0).(*array.BooleanBuilder).AppendValues(expectedValues[20:30], nil)
	rec3 := b.NewRecord()
	defer rec3.Release()

	records := []array.Record{rec1, rec2, rec3}
	tbl := array.NewTableFromRecords(schema, records)
	defer tbl.Release()
	column := tbl.Column(0)
	vr := iterator.NewBooleanValueIterator(column)
	defer vr.Release()

	n := 0
	for vr.Next() {
		value, null := vr.Value()
		if got, want := value, expectedValues[n]; got != want {
			t.Fatalf("got=%t, want=%t", got, want)
		}
		if got, want := !null, expectedValuesBool[n]; got != want {
			t.Fatalf("got=%v, want=%v (n=%d)", got, want, n)
		}
		n++
	}
}

func TestStringValueIterator(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "c1-strings", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	expectedValues := []string{
		"true", "aaa", "true", "true", "true", "ccc", "true", "d", "true", "e",
		"false", "false", "false", "false", "false", "false", "false", "dog", "false", "false",
		"true", "true", "bbb", "true", "true", "true", "true", "true", "cat", "true",
	}

	expectedValuesBool := []bool{
		true, true, true, true, true, true, true, true, true, true,
		true, false, true, false, true, true, true, true, true, false,
		true, true, true, true, true, true, true, true, true, true,
	}

	b.Field(0).(*array.StringBuilder).AppendValues(expectedValues[0:10], nil)
	rec1 := b.NewRecord()
	defer rec1.Release()

	b.Field(0).(*array.StringBuilder).AppendValues(expectedValues[10:20], expectedValuesBool[10:20])
	rec2 := b.NewRecord()
	defer rec2.Release()

	b.Field(0).(*array.StringBuilder).AppendValues(expectedValues[20:30], nil)
	rec3 := b.NewRecord()
	defer rec3.Release()

	records := []array.Record{rec1, rec2, rec3}
	tbl := array.NewTableFromRecords(schema, records)
	defer tbl.Release()
	column := tbl.Column(0)
	vr := iterator.NewStringValueIterator(column)
	defer vr.Release()

	n := 0
	for vr.Next() {
		value, null := vr.Value()
		if got, want := value, expectedValues[n]; got != want {
			t.Fatalf("got=%s, want=%s", got, want)
		}
		if got, want := !null, expectedValuesBool[n]; got != want {
			t.Fatalf("got=%v, want=%v (n=%d)", got, want, n)
		}
		n++
	}
}

func TestValueAsJSON(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	for _, tc := range []struct {
		name     string
		iterator iterator.ValueIterator
		result   string
		err      error
	}{
		{
			name: "int32 test",
			iterator: func() iterator.ValueIterator {
				ib := array.NewInt32Builder(mem)
				defer ib.Release()

				ib.AppendValues([]int32{123}, nil)
				i1 := ib.NewInt32Array()
				defer i1.Release()

				chunk := array.NewChunked(
					arrow.PrimitiveTypes.Int32,
					[]array.Interface{i1},
				)
				defer chunk.Release()

				field := arrow.Field{Name: "i32", Type: arrow.PrimitiveTypes.Int32}
				col := array.NewColumn(field, chunk)
				defer col.Release()

				return iterator.NewValueIterator(col)
			}(),
			result: `123`,
			err:    nil,
		},
		{
			name: "string test",
			iterator: func() iterator.ValueIterator {
				ib := array.NewStringBuilder(mem)
				defer ib.Release()

				ib.AppendValues([]string{"foo bar"}, nil)
				i1 := ib.NewStringArray()
				defer i1.Release()

				chunk := array.NewChunked(
					arrow.BinaryTypes.String,
					[]array.Interface{i1},
				)
				defer chunk.Release()

				field := arrow.Field{Name: "str", Type: arrow.BinaryTypes.String}
				col := array.NewColumn(field, chunk)
				defer col.Release()

				return iterator.NewValueIterator(col)
			}(),
			result: `"foo bar"`,
			err:    nil,
		},
		{
			name: "float16 test",
			iterator: func() iterator.ValueIterator {
				ib := array.NewFloat16Builder(mem)
				defer ib.Release()

				ib.AppendValues(f16sFrom([]float32{1}), nil)
				i1 := ib.NewFloat16Array()
				defer i1.Release()

				chunk := array.NewChunked(
					arrow.FixedWidthTypes.Float16,
					[]array.Interface{i1},
				)
				defer chunk.Release()

				field := arrow.Field{Name: "f16", Type: arrow.FixedWidthTypes.Float16}
				col := array.NewColumn(field, chunk)
				defer col.Release()

				return iterator.NewValueIterator(col)
			}(),
			result: `1`,
			err:    nil,
		},
		{
			name: "list of string test",
			iterator: func() iterator.ValueIterator {
				lb := array.NewListBuilder(mem, arrow.BinaryTypes.String)
				defer lb.Release()

				vb := lb.ValueBuilder().(*array.StringBuilder)
				lb.Append(true)
				vb.Append("foo")
				vb.Append("bar")
				lb.Append(false)
				lb.Append(true)
				vb.Append("ping")

				i1 := lb.NewListArray()
				defer i1.Release()

				chunk := array.NewChunked(
					arrow.ListOf(arrow.BinaryTypes.String),
					[]array.Interface{i1},
				)
				defer chunk.Release()

				field := arrow.Field{Name: "los", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true}
				col := array.NewColumn(field, chunk)
				defer col.Release()

				return iterator.NewValueIterator(col)
			}(),
			result: `["foo","bar"]`,
			err:    nil,
		},
		{
			name: "struct of string fields test",
			iterator: func() iterator.ValueIterator {
				dt := arrow.StructOf([]arrow.Field{
					{Name: "field1", Type: arrow.BinaryTypes.String},
					{Name: "field2", Type: arrow.BinaryTypes.String},
				}...)
				sb := array.NewStructBuilder(mem, dt)
				defer sb.Release()
				fb0 := sb.FieldBuilder(0).(*array.StringBuilder)
				fb1 := sb.FieldBuilder(1).(*array.StringBuilder)

				sb.Append(true)
				fb0.Append("foo")
				fb1.Append("bar")
				sb.Append(false)
				sb.Append(true)
				fb0.Append("ping")
				fb1.Append("pong")

				s1 := sb.NewStructArray()
				defer s1.Release()

				chunk := array.NewChunked(
					dt,
					[]array.Interface{s1},
				)
				defer chunk.Release()

				field := arrow.Field{Name: "sofstr", Type: dt, Nullable: true}
				col := array.NewColumn(field, chunk)
				defer col.Release()

				return iterator.NewValueIterator(col)
			}(),
			result: `{"field1":"foo","field2":"bar"}`,
			err:    nil,
		},
		{
			name: "list of list of string test",
			iterator: func() iterator.ValueIterator {
				lb := array.NewListBuilder(mem, arrow.ListOf(arrow.BinaryTypes.String))
				defer lb.Release()

				lb2 := lb.ValueBuilder().(*array.ListBuilder)
				vb := lb2.ValueBuilder().(*array.StringBuilder)
				// [[[foo,bar],null,[ping,pong]],null,[beep]]
				lb.Append(true)
				lb2.Append(true)
				vb.Append("foo")
				vb.Append("bar")
				lb2.Append(false)
				lb2.Append(true)
				vb.Append("ping")
				vb.Append("pong")
				lb.Append(false)
				lb.Append(true)
				lb2.Append(true)
				vb.Append("beep")

				i1 := lb.NewListArray()
				defer i1.Release()

				chunk := array.NewChunked(
					arrow.ListOf(arrow.ListOf(arrow.BinaryTypes.String)),
					[]array.Interface{i1},
				)
				defer chunk.Release()

				field := arrow.Field{Name: "los", Type: arrow.ListOf(arrow.ListOf(arrow.BinaryTypes.String)), Nullable: true}
				col := array.NewColumn(field, chunk)
				defer col.Release()

				return iterator.NewValueIterator(col)
			}(),
			result: `[["foo","bar"],null,["ping","pong"]]`,
			err:    nil,
		},
		{
			name: "struct of struct of string fields test",
			iterator: func() iterator.ValueIterator {
				dt := arrow.StructOf([]arrow.Field{
					{Name: "field1", Type: arrow.StructOf([]arrow.Field{
						{Name: "fielda", Type: arrow.BinaryTypes.String},
						{Name: "fieldb", Type: arrow.BinaryTypes.String},
					}...)},
				}...)
				sb := array.NewStructBuilder(mem, dt)
				defer sb.Release()

				sb2 := sb.FieldBuilder(0).(*array.StructBuilder)
				fb0 := sb2.FieldBuilder(0).(*array.StringBuilder)
				fb1 := sb2.FieldBuilder(1).(*array.StringBuilder)

				// [{"field1":{"fielda":"foo","fieldb":"bar"}},null,{"field1":{"fielda":"ping","fieldb":"pong"}}]
				sb.Append(true)
				sb2.Append(true)
				fb0.Append("foo")
				fb1.Append("bar")

				sb.Append(false)

				sb.Append(true)
				sb2.Append(true)
				fb0.Append("ping")
				fb1.Append("pong")

				s1 := sb.NewStructArray()
				defer s1.Release()

				chunk := array.NewChunked(
					dt,
					[]array.Interface{s1},
				)
				defer chunk.Release()

				field := arrow.Field{Name: "sofstr", Type: dt, Nullable: true}
				col := array.NewColumn(field, chunk)
				defer col.Release()

				return iterator.NewValueIterator(col)
			}(),
			result: `{"field1":{"fielda":"foo","fieldb":"bar"}}`,
			err:    nil,
		},
		{
			name: "list of struct of string test",
			iterator: func() iterator.ValueIterator {
				dt := arrow.StructOf([]arrow.Field{
					{Name: "field1", Type: arrow.BinaryTypes.String},
					{Name: "field2", Type: arrow.BinaryTypes.String},
				}...)
				lb := array.NewListBuilder(mem, dt)
				defer lb.Release()

				sb := lb.ValueBuilder().(*array.StructBuilder)
				fb0 := sb.FieldBuilder(0).(*array.StringBuilder)
				fb1 := sb.FieldBuilder(1).(*array.StringBuilder)

				// [[{"field1":"foo","field2":"bar"},null,{"field1":"ping","field2":"pong"}],null,[{"field1":"beep","field2":"boop"}]]
				lb.Append(true)
				sb.Append(true)
				fb0.Append("foo")
				fb1.Append("bar")
				sb.Append(false)
				sb.Append(true)
				fb0.Append("ping")
				fb1.Append("pong")

				lb.Append(false)

				lb.Append(true)
				sb.Append(true)
				fb0.Append("beep")
				fb1.Append("boop")

				i1 := lb.NewListArray()
				defer i1.Release()

				chunk := array.NewChunked(
					arrow.ListOf(dt),
					[]array.Interface{i1},
				)
				defer chunk.Release()

				field := arrow.Field{Name: "los", Type: arrow.ListOf(dt), Nullable: true}
				col := array.NewColumn(field, chunk)
				defer col.Release()

				return iterator.NewValueIterator(col)
			}(),
			result: `[{"field1":"foo","field2":"bar"},null,{"field1":"ping","field2":"pong"}]`,
			err:    nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer tc.iterator.Release()
			tc.iterator.Next()

			res, err := tc.iterator.ValueAsJSON()
			if err != tc.err {
				t.Errorf("got err=%v, want err=%v for test: %s\n", err, tc.err, tc.name)
			}

			// marshal the result
			resultBytes, err := json.Marshal(res)
			if err != tc.err {
				t.Errorf("got error marshaling json for test: %s\n%v", tc.name, err)
			}

			result := string(resultBytes)
			if result != tc.result {
				t.Errorf("got result=%s, want result=%s for test: %s | %T - %T\n", result, tc.result, tc.name, result, tc.result)
			}
		})
	}
}

func f16sFrom(vs []float32) []float16.Num {
	o := make([]float16.Num, len(vs))
	for i, v := range vs {
		o[i] = float16.New(v)
	}
	return o
}

// Used to verify we were building the arrow structures correctly.
func getColumnFromFile(t *testing.T, mem memory.Allocator) iterator.ValueIterator {
	t.Helper()
	// Try reading in the los.arrow file
	f, err := os.Open("/Users/nick/projects/bullseye/tmp/los.arrow")
	if err != nil {
		t.Fatal(err)
	}
	r, err := ipc.NewFileReader(f, ipc.WithAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	recs := make([]array.Record, r.NumRecords())
	for i := 0; i < r.NumRecords(); i++ {
		rec, err := r.Record(i)
		if err != nil {
			t.Fatalf("could not read record %d: %v", i, err)
		}
		recs[0] = rec
	}

	tbl := array.NewTableFromRecords(recs[0].Schema(), recs)
	defer tbl.Release()

	col := tbl.Column(0)

	return iterator.NewValueIterator(col)
}

func TestSliceArray(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	for _, tc := range []struct {
		name  string
		want  string
		arr   array.Interface
		check func(arr array.Interface) string
	}{
		{
			name: "SliceArray int32",
			want: "[1 2 3]",
			arr: func() array.Interface {
				ib := array.NewInt32Builder(mem)
				defer ib.Release()

				ib.AppendValues([]int32{1, 2, 3, 4}, nil)
				i1 := ib.NewInt32Array()
				return i1
			}(),
			check: func(arr array.Interface) string {
				t.Helper()
				vals := arr.(*array.Int32)
				return vals.String()
			},
		},
		{
			name: "SliceArray struct",
			want: "{[1 (null) 3] [2 (null) 4] [1 (null) 3] [2 (null) 4]}",
			arr: func() array.Interface {
				fields := []arrow.Field{
					{Name: "field1", Type: arrow.PrimitiveTypes.Int32},
					{Name: "field2", Type: arrow.PrimitiveTypes.Int32},
					{Name: "field3", Type: arrow.PrimitiveTypes.Int32},
					{Name: "field4", Type: arrow.PrimitiveTypes.Int32},
				}
				sb := array.NewStructBuilder(mem, arrow.StructOf(fields...))
				defer sb.Release()
				fb0 := sb.FieldBuilder(0).(*array.Int32Builder)
				fb1 := sb.FieldBuilder(1).(*array.Int32Builder)
				fb2 := sb.FieldBuilder(2).(*array.Int32Builder)
				fb3 := sb.FieldBuilder(3).(*array.Int32Builder)

				sb.Append(true)
				fb0.Append(1)
				fb1.Append(2)
				fb2.Append(1)
				fb3.Append(2)

				sb.Append(false)

				sb.Append(true)
				fb0.Append(3)
				fb1.Append(4)
				fb2.Append(3)
				fb3.Append(4)

				sb.Append(true)
				fb0.Append(5)
				fb1.Append(6)
				fb2.Append(5)
				fb3.Append(6)

				sb.Append(true)
				fb0.Append(7)
				fb1.Append(8)
				fb2.Append(7)
				fb3.Append(8)

				s1 := sb.NewStructArray()
				return s1
			}(),
			check: func(arr array.Interface) string {
				t.Helper()
				vals := arr.(*array.Struct)
				return vals.String()
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer tc.arr.Release()
			beg := 0
			end := 3
			arr := array.NewSlice(tc.arr, int64(beg), int64(end))
			defer arr.Release()
			got := tc.check(arr)
			if got != tc.want {
				t.Errorf("\ngot=%#v\nwant=%#v", got, tc.want)
			}
		})
	}
}
