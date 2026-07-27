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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// These tests cover tableReaderFacade.AddColumn, which satisfies the
// arrow.Table interface added in Arrow for Go v18. They pin down the contract
// surfaced during the v18 upgrade: column length must match the table's row
// count, the field type must match the column type, position must be in range,
// and the original schema's metadata/endianness must be preserved. Every case
// runs under a CheckedAllocator so a ref-count imbalance fails the test.

// buildFacadeDF builds a simple one-column DataFrame backed by a CheckedAllocator
// and returns the facade, the DataFrame, and the allocator so the caller can
// defer pool.AssertSize / df.Release.
func buildFacadeDF(t *testing.T) (TableFacade, *DataFrame, *memory.CheckedAllocator) {
	t.Helper()
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())

	col, err := NewColumnFromMem(pool, "col0", []int32{1, 2, 3})
	if err != nil {
		pool.AssertSize(t, 0)
		t.Fatalf("build column: %v", err)
	}
	defer col.Release()

	df, err := NewDataFrameFromColumns(pool, []arrow.Column{*col})
	if err != nil {
		pool.AssertSize(t, 0)
		t.Fatalf("build dataframe: %v", err)
	}

	return NewTableFacade(df), df, pool
}

func TestTableFacadeAddColumnMiddle(t *testing.T) {
	facade, df, pool := buildFacadeDF(t)
	defer pool.AssertSize(t, 0)
	defer df.Release()

	// Build a second compatible column (int32, same length) to insert at pos 1.
	col1, err := NewColumnFromMem(pool, "col1", []int32{4, 5, 6})
	if err != nil {
		t.Fatalf("build column: %v", err)
	}
	defer col1.Release()

	tbl, err := facade.AddColumn(1, arrow.Field{Name: "col1", Type: arrow.PrimitiveTypes.Int32}, *col1)
	if err != nil {
		t.Fatalf("AddColumn: %v", err)
	}
	defer tbl.Release()

	if got, want := tbl.NumCols(), int64(2); got != want {
		t.Fatalf("NumCols: got=%d, want=%d", got, want)
	}
	if got, want := tbl.NumRows(), int64(3); got != want {
		t.Fatalf("NumRows: got=%d, want=%d", got, want)
	}
	if got, want := tbl.Schema().Field(0).Name, "col0"; got != want {
		t.Fatalf("field 0 name: got=%q, want=%q", got, want)
	}
	if got, want := tbl.Schema().Field(1).Name, "col1"; got != want {
		t.Fatalf("field 1 name: got=%q, want=%q", got, want)
	}
}

func TestTableFacadeAddColumnPreservesSchemaMetadata(t *testing.T) {
	// Attach metadata to the DataFrame's schema and confirm AddColumn keeps it
	// (a plain arrow.NewSchema(fields, nil) would drop it).
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	col, err := NewColumnFromMem(pool, "col0", []int32{1, 2, 3})
	if err != nil {
		t.Fatalf("build column: %v", err)
	}
	defer col.Release()

	// Build a DataFrame then re-wrap its schema with metadata.
	df, err := NewDataFrameFromColumns(pool, []arrow.Column{*col})
	if err != nil {
		t.Fatalf("build dataframe: %v", err)
	}
	defer df.Release()

	metaFields := df.Schema().Fields()
	meta := arrow.NewMetadata([]string{"owner"}, []string{"gomem"})
	metaSchema := arrow.NewSchema(metaFields, &meta)
	metaDf, err := NewDataFrame(pool, metaSchema, []arrow.Array{df.ColumnAt(0).Data().Chunk(0)})
	if err != nil {
		t.Fatalf("build metadata dataframe: %v", err)
	}
	defer metaDf.Release()

	facade := NewTableFacade(metaDf)

	col1, err := NewColumnFromMem(pool, "col1", []int32{7, 8, 9})
	if err != nil {
		t.Fatalf("build column: %v", err)
	}
	defer col1.Release()

	tbl, err := facade.AddColumn(1, arrow.Field{Name: "col1", Type: arrow.PrimitiveTypes.Int32}, *col1)
	if err != nil {
		t.Fatalf("AddColumn: %v", err)
	}
	defer tbl.Release()

	got, ok := tbl.Schema().Metadata().GetValue("owner")
	if !ok {
		t.Fatalf("schema metadata dropped: key %q missing", "owner")
	}
	if want := "gomem"; got != want {
		t.Fatalf("schema metadata: got=%q, want=%q", got, want)
	}
}

func TestTableFacadeAddColumnLengthMismatch(t *testing.T) {
	facade, df, pool := buildFacadeDF(t)
	defer pool.AssertSize(t, 0)
	defer df.Release()

	// The table has 3 rows; this column has only 2. AddColumn must return an
	// error rather than panicking inside array.NewTable.
	tooShort, err := NewColumnFromMem(pool, "col1", []int32{4, 5})
	if err != nil {
		t.Fatalf("build column: %v", err)
	}
	defer tooShort.Release()

	tbl, err := facade.AddColumn(1, arrow.Field{Name: "col1", Type: arrow.PrimitiveTypes.Int32}, *tooShort)
	if err == nil {
		tbl.Release()
		t.Fatal("expected length-mismatch error, got nil")
	}
	if tbl != nil {
		tbl.Release()
		t.Fatalf("expected nil table on error, got %v", tbl)
	}
}

func TestTableFacadeAddColumnTypeMismatch(t *testing.T) {
	facade, df, pool := buildFacadeDF(t)
	defer pool.AssertSize(t, 0)
	defer df.Release()

	// int32 column but a float64 field: must error rather than panic.
	col1, err := NewColumnFromMem(pool, "col1", []int32{4, 5, 6})
	if err != nil {
		t.Fatalf("build column: %v", err)
	}
	defer col1.Release()

	tbl, err := facade.AddColumn(1, arrow.Field{Name: "col1", Type: arrow.PrimitiveTypes.Float64}, *col1)
	if err == nil {
		tbl.Release()
		t.Fatal("expected type-mismatch error, got nil")
	}
	if tbl != nil {
		tbl.Release()
		t.Fatalf("expected nil table on error, got %v", tbl)
	}
}

func TestTableFacadeAddColumnPositionOutOfBounds(t *testing.T) {
	facade, df, pool := buildFacadeDF(t)
	defer pool.AssertSize(t, 0)
	defer df.Release()

	col1, err := NewColumnFromMem(pool, "col1", []int32{4, 5, 6})
	if err != nil {
		t.Fatalf("build column: %v", err)
	}
	defer col1.Release()

	for _, pos := range []int{-1, 2} {
		tbl, err := facade.AddColumn(pos, arrow.Field{Name: "col1", Type: arrow.PrimitiveTypes.Int32}, *col1)
		if err == nil {
			tbl.Release()
			t.Fatalf("pos=%d: expected out-of-range error, got nil", pos)
		}
		if tbl != nil {
			tbl.Release()
			t.Fatalf("pos=%d: expected nil table on error, got %v", pos, tbl)
		}
	}
}
