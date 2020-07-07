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
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/gomem/gomem/pkg/iterator"
)

func buildRecords(pool *memory.CheckedAllocator, t *testing.T) ([]array.Record, *arrow.Schema) {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			{Name: "f2-f64", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	b.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3, 4, 5, 6}, nil)
	b.Field(0).(*array.Int32Builder).AppendValues([]int32{7, 8, 9, 10}, []bool{true, true, false, true})
	b.Field(1).(*array.Float64Builder).AppendValues([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)

	rec1 := b.NewRecord()

	b.Field(0).(*array.Int32Builder).AppendValues([]int32{11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, nil)
	b.Field(1).(*array.Float64Builder).AppendValues([]float64{11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, nil)

	rec2 := b.NewRecord()

	b.Field(0).(*array.Int32Builder).AppendValues([]int32{31, 32, 33, 34, 35, 36, 37, 38, 39, 40}, nil)
	b.Field(1).(*array.Float64Builder).AppendValues([]float64{31, 32, 33, 34, 35, 36, 37, 38, 39, 40}, nil)

	rec3 := b.NewRecord()

	return []array.Record{rec1, rec2, rec3}, schema
}

func TestChunkIterator(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	records, schema := buildRecords(pool, t)
	defer func() {
		for i := range records {
			records[i].Release()
		}
	}()

	expectedPtrs := make([]*int32, len(records))
	for i := range expectedPtrs {
		expectedPtrs[i] = &records[i].Column(0).(*array.Int32).Int32Values()[0]
	}

	tbl := array.NewTableFromRecords(schema, records)
	defer tbl.Release()

	column := tbl.Column(0)
	cr := iterator.NewChunkIterator(column)
	defer cr.Release()

	n := 0
	for cr.Next() {
		values := cr.Chunk().(*array.Int32).Int32Values()
		if got, want := &values[0], expectedPtrs[n]; got != want {
			t.Fatalf("got=%d, want=%d", got, want)
		}
		n++
	}
}

func TestInt32ChunkIterator(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	records, schema := buildRecords(pool, t)
	defer func() {
		for i := range records {
			records[i].Release()
		}
	}()

	expectedPtrs := make([]*int32, len(records))
	for i := range expectedPtrs {
		expectedPtrs[i] = &records[i].Column(0).(*array.Int32).Int32Values()[0]
	}

	tbl := array.NewTableFromRecords(schema, records)
	defer tbl.Release()

	column := tbl.Column(0)
	cr := iterator.NewInt32ChunkIterator(column)
	defer cr.Release()

	n := 0
	for cr.Next() {
		values := cr.ChunkValues()
		if got, want := &values[0], expectedPtrs[n]; got != want {
			t.Fatalf("got=%d, want=%d", got, want)
		}
		n++
	}
}
