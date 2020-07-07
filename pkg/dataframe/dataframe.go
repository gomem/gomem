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
	"sort"
	"strings"
	"sync/atomic"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/gomem/gomem/internal/constructors"
	"github.com/gomem/gomem/internal/debug"
	"github.com/gomem/gomem/pkg/iterator"
	"github.com/gomem/gomem/pkg/smartbuilder"
)

// Dict is a map of string to array of data.
type Dict map[string]interface{}

// Option is an option that may be passed to a function.
type Option func(interface{}) error

// NewDataFrame creates a new data frame from the provided schema and arrays.
func NewDataFrame(mem memory.Allocator, schema *arrow.Schema, arrs []array.Interface) (*DataFrame, error) {
	df := &DataFrame{
		refs:    1,
		mem:     mem,
		schema:  schema,
		rows:    -1,
		mutator: NewMutator(mem),
	}

	if df.rows < 0 {
		switch len(arrs) {
		case 0:
			df.rows = 0
		default:
			df.rows = int64(arrs[0].Len())
		}
	}

	if df.schema == nil {
		return nil, fmt.Errorf("dataframe: nil schema")
	}

	if len(df.schema.Fields()) != len(arrs) {
		return nil, fmt.Errorf("dataframe: inconsistent schema/arrays")
	}

	for i, arr := range arrs {
		ft := df.schema.Field(i)
		if fmt.Sprintf("%s", arr.DataType()) != fmt.Sprintf("%s", ft.Type) {
			return nil, fmt.Errorf("dataframe: column %q is inconsitent with schema (%s != %s)", ft.Name, arr.DataType(), ft.Type)
		}

		if int64(arr.Len()) < df.rows {
			return nil, fmt.Errorf("dataframe: column %q expected length >= %d but got length %d", ft.Name, df.rows, arr.Len())
		}
	}

	df.cols = make([]array.Column, len(arrs))
	for i := range arrs {
		func(i int) {
			chunk := array.NewChunked(arrs[i].DataType(), []array.Interface{arrs[i]})
			defer chunk.Release()

			col := array.NewColumn(df.schema.Field(i), chunk)
			df.cols[i] = *col
		}(i)
	}

	return df, nil
}

// NewDataFrameFromColumns returns a DataFrame interface.
func NewDataFrameFromColumns(mem memory.Allocator, cols []array.Column) (*DataFrame, error) {
	var rows int64
	if len(cols) > 0 {
		rows = columnLen(cols[0])
	}

	return NewDataFrameFromShape(mem, cols, rows)
}

// NewDataFrameFromMem creates a new data frame from the provided in-memory data.
func NewDataFrameFromMem(mem memory.Allocator, dict Dict) (*DataFrame, error) {
	var (
		err    error
		arrs   = make([]array.Interface, 0, len(dict))
		fields = make([]arrow.Field, 0, len(dict))
	)

	keys := make([]string, 0, len(dict))
	for k := range dict {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := dict[k]
		arr, field, newInterfaceErr := constructors.NewInterfaceFromMem(mem, k, v, nil)
		if newInterfaceErr != nil {
			err = newInterfaceErr
			break
		}
		arrs = append(arrs, arr)
		fields = append(fields, *field)
	}

	defer func() {
		for i := range arrs {
			arrs[i].Release()
		}
	}()

	if err != nil {
		return nil, err
	}

	schema := arrow.NewSchema(fields, nil)
	return NewDataFrame(mem, schema, arrs)
}

// NewDataFrameFromShape is the same as NewDataFrameFromColumns only it allows you to specify the number
// of rows in the DataFrame.
func NewDataFrameFromShape(mem memory.Allocator, cols []array.Column, rows int64) (*DataFrame, error) {
	df := &DataFrame{
		refs:    1,
		mem:     mem,
		schema:  buildSchema(cols),
		cols:    cols,
		rows:    rows,
		mutator: NewMutator(mem),
	}

	// validate the data frame and its constituents.
	// note we retain the columns after having validated the data frame
	// in case the validation fails and panics (and would otherwise leak
	// a ref-count on the columns.)
	if err := df.validate(); err != nil {
		return nil, err
	}

	for i := range df.cols {
		df.cols[i].Retain()
	}

	return df, nil
}

func NewDataFrameFromTable(mem memory.Allocator, table array.Table) (*DataFrame, error) {
	cols := make([]array.Column, table.NumCols())
	for i := range cols {
		col := table.Column(i)
		cols[i] = *col
	}

	return NewDataFrameFromShape(mem, cols, table.NumRows())
}

func NewDataFrameFromRecord(mem memory.Allocator, record array.Record) (*DataFrame, error) {
	return NewDataFrame(mem, record.Schema(), record.Columns())
}

// DataFrame is an immutable DataFrame that uses Arrow
// to store it's data in a standard columnar format.
type DataFrame struct {
	refs   int64 // reference count
	mem    memory.Allocator
	schema *arrow.Schema

	cols []array.Column
	rows int64

	// Mutations that can be performed on this DataFrame
	// require a the Mutator to be set up.
	mutator *Mutator
}

// Allocator returns the memory allocator for this DataFrame
func (df *DataFrame) Allocator() memory.Allocator {
	return df.mem
}

// Column returns the column matching the given name.
func (df *DataFrame) Column(name string) *array.Column {
	for i, col := range df.cols {
		if col.Name() == name {
			return &df.cols[i]
		}
	}
	return nil
}

// ColumnAt returns the i-th column of this Frame.
func (df *DataFrame) ColumnAt(i int) *array.Column {
	return &df.cols[i]
}

// Columns is the slice of Columns that make up this DataFrame.
func (df *DataFrame) Columns() []array.Column {
	return df.cols
}

// ColumnNames is the slice of column names that make up this DataFrame.
func (df *DataFrame) ColumnNames() []string {
	fields := df.schema.Fields()
	names := make([]string, len(fields))
	for i, field := range fields {
		names[i] = field.Name
	}
	return names
}

// ColumnTypes is the slice of column types that make up this DataFrame.
func (df *DataFrame) ColumnTypes() []arrow.Field {
	return df.schema.Fields()
}

// Equals checks for equality between this DataFrame and DataFrame d.
// nil elements at the same location are considered equal.
func (df *DataFrame) Equals(d *DataFrame) bool {
	if !df.schema.Equal(d.schema) {
		return false
	}

	// compare the columns
	leftCols := df.Columns()
	rightCols := d.Columns()

	if len(leftCols) != len(rightCols) {
		return false
	}

	for i := range leftCols {
		leftCol := leftCols[i]
		rightCol := rightCols[i]

		// Could do this with a column iterator?
		same := compareColumns(&leftCol, &rightCol)
		if !same {
			return false
		}
	}

	return true
}

// NumCols returns the number of columns of this DataFrame using Go's len().
func (df *DataFrame) NumCols() int {
	return len(df.cols)
}

// NumRows returns the number of rows of this DataFrame.
func (df *DataFrame) NumRows() int64 {
	return df.rows
}

// Name returns the name of the i-th column of this DataFrame.
func (df *DataFrame) Name(i int) string {
	return df.schema.Field(i).Name
}

// Dims retrieves the dimensions of a DataFrame.
func (df *DataFrame) Dims() (int, int64) {
	return len(df.cols), df.rows
}

// Display builds out a string representation of the DataFrame that is useful for debugging.
// if chunkSize is <= 0, the biggest possible chunk will be selected.
func (df *DataFrame) Display(chunkSize int64) string {
	tr := array.NewTableReader(NewTableFacade(df), chunkSize)
	defer tr.Release()

	n := 0
	var output strings.Builder
	for tr.Next() {
		rec := tr.Record()
		for i, col := range rec.Columns() {
			fmt.Fprintf(&output, "rec[%d][%q]: %v\n", n, rec.ColumnName(i), col)
		}
		n++
	}

	return output.String()
}

/**
 * These are column specific helpers
 */

// SelectColumns returns only columns matching names.
func (df *DataFrame) SelectColumns(names ...string) []array.Column {
	if len(names) == 0 {
		return []array.Column{}
	}

	set := make(map[string]struct{}, len(names))
	for _, name := range names {
		set[name] = struct{}{}
	}

	cols := make([]array.Column, 0, len(names))

	dfColumns := df.Columns()
	for i := range dfColumns {
		if _, ok := set[dfColumns[i].Name()]; !ok {
			continue
		}
		cols = append(cols, dfColumns[i])
	}

	return cols[:len(cols):len(cols)]
}

// RejectColumns returns only columns not matching names.
func (df *DataFrame) RejectColumns(names ...string) []array.Column {
	if len(names) == 0 {
		return df.Columns()
	}

	set := make(map[string]struct{}, len(names))
	for _, name := range names {
		set[name] = struct{}{}
	}

	cols := make([]array.Column, 0, df.NumCols()-len(names))

	dfColumns := df.Columns()
	for i := range dfColumns {
		if _, drop := set[dfColumns[i].Name()]; drop {
			continue
		}
		cols = append(cols, dfColumns[i])
	}

	return cols[:len(cols):len(cols)]
}

// Apply takes a series of MutationFunc and calls them with the existing DataFrame on the left.
func (df *DataFrame) Apply(fns ...MutationFunc) (*DataFrame, error) {
	left, err := df.Copy()
	if err != nil {
		return nil, err
	}
	if len(fns) == 0 {
		return left, err
	}
	for i := range fns {
		left, err = func() (*DataFrame, error) {
			defer left.Release()
			return fns[i](left)
		}()
		if err != nil {
			return nil, err
		}
	}
	return left, err
}

// ApplyToColumnFunc is a type alias for a function that will be called for each element
// that is iterated over in a column. The return value will
type ApplyToColumnFunc func(v interface{}) (interface{}, error)

// ApplyToColumn creates a new DataFrame with the new column appended. The new column is built
// with the response values obtained from ApplyToColumnFunc. An error response value from
// ApplyToColumnFunc will cause ApplyToColumn to return immediately.
func (df *DataFrame) ApplyToColumn(columnName, newColumnName string, fn ApplyToColumnFunc) (*DataFrame, error) {
	return df.Apply(func(df *DataFrame) (*DataFrame, error) {
		// TODO(nickpoorman): refactor this
		col := df.Column(columnName)
		field := col.Field()
		field.Name = newColumnName
		schema := arrow.NewSchema([]arrow.Field{field}, nil)
		builder := array.NewRecordBuilder(df.Allocator(), schema)
		defer builder.Release()
		smartBuilder := smartbuilder.NewSmartBuilder(builder)
		valueIterator := iterator.NewValueIterator(col)
		defer valueIterator.Release()
		for valueIterator.Next() {
			value := valueIterator.ValueInterface()
			res, err := fn(value)
			if err != nil {
				return nil, err
			}
			smartBuilder.Append(0, res)
		}
		rec := builder.NewRecord()
		defer rec.Release()
		chunk := array.NewChunked(col.DataType(), rec.Columns())
		defer chunk.Release()
		newCol := array.NewColumn(field, chunk)
		defer newCol.Release()
		return df.AppendColumn(newCol)
	})
}

/**
 * The following functions will always return a new DataFrame.
 */

// AppendColumn builds a new DataFrame with the provided Column included.
func (df *DataFrame) AppendColumn(c *array.Column) (*DataFrame, error) {
	nCols := len(df.cols)
	cols := make([]array.Column, nCols+1)
	copy(cols, df.cols)
	cols[nCols] = *c
	return NewDataFrameFromShape(df.mem, cols, df.rows)
}

// Copy returns a copy of this dataframe. The underlying byte buffers will not be copied.
func (df *DataFrame) Copy() (*DataFrame, error) {
	nCols := len(df.cols)
	cols := make([]array.Column, nCols)
	copy(cols, df.cols)
	return NewDataFrameFromShape(df.mem, cols, df.rows)
}

// CrossJoin returns a DataFrame containing the cross join of two DataFrames.
func (df *DataFrame) CrossJoin(right *DataFrame, opts ...Option) (*DataFrame, error) {
	fn := df.mutator.CrossJoin(right, opts...)
	return fn(df)
}

// Select the given DataFrame columns by name.
func (df *DataFrame) Select(names ...string) (*DataFrame, error) {
	fn := df.mutator.Select(names...)
	return fn(df)
}

// Drop the given DataFrame columns by name.
func (df *DataFrame) Drop(names ...string) (*DataFrame, error) {
	fn := df.mutator.Drop(names...)
	return fn(df)
}

// InnerJoin returns a DataFrame containing the inner join of two DataFrames.
func (df *DataFrame) InnerJoin(right *DataFrame, columns []string, opts ...Option) (*DataFrame, error) {
	fn := df.mutator.InnerJoin(right, columns, opts...)
	return fn(df)
}

// LeftJoin returns a DataFrame containing the left join of two DataFrames.
func (df *DataFrame) LeftJoin(right *DataFrame, columns []string, opts ...Option) (*DataFrame, error) {
	fn := df.mutator.LeftJoin(right, columns, opts...)
	return fn(df)
}

// OuterJoin returns a DataFrame containing the outer join of two DataFrames.
// Use union of keys from both frames, similar to a SQL full outer join.
func (df *DataFrame) OuterJoin(right *DataFrame, columns []string, opts ...Option) (*DataFrame, error) {
	fn := df.mutator.OuterJoin(right, columns, opts...)
	return fn(df)
}

// RightJoin returns a DataFrame containing the right join of two DataFrames.
func (df *DataFrame) RightJoin(right *DataFrame, columns []string, opts ...Option) (*DataFrame, error) {
	fn := df.mutator.RightJoin(right, columns, opts...)
	return fn(df)
}

// Slice creates a new DataFrame consisting of rows[beg:end].
func (df *DataFrame) Slice(beg, end int64) (*DataFrame, error) {
	return df.mutator.Slice(beg, end)(df)
}

// Schema returns the schema of this Frame.
func (df *DataFrame) Schema() *arrow.Schema {
	return df.schema
}

// Retain increases the reference count by 1.
// Retain may be called simultaneously from multiple goroutines.
func (df *DataFrame) Retain() {
	atomic.AddInt64(&df.refs, 1)
}

// Release decreases the reference count by 1.
// When the reference count goes to zero, the memory is freed.
// Release may be called simultaneously from multiple goroutines.
func (df *DataFrame) Release() {
	refs := atomic.AddInt64(&df.refs, -1)
	debug.Assert(refs >= 0, "too many releases")

	if refs == 0 {
		for i := range df.cols {
			df.cols[i].Release()
		}
		df.cols = nil
	}
}

func (df *DataFrame) validate() error {
	if len(df.Columns()) != len(df.schema.Fields()) {
		return errors.New("dataframe validate(): table schema mismatch")
	}
	for i, col := range df.cols {
		if !col.Field().Equal(df.schema.Field(i)) {
			return fmt.Errorf("dataframe validate(): column field %q is inconsistent with schema", col.Name())
		}
		colLen := columnLen(col)
		if colLen < df.rows {
			return fmt.Errorf("dataframe validate(): column %q expected length >= %d but got length %d", col.Name(), df.rows, colLen)
		}
	}
	return nil
}

func compareColumns(left, right *array.Column) bool {
	// We have to use value iterators and the only way to do that is to switch on the type
	leftDtype := left.DataType()
	rightDtype := right.DataType()
	if leftDtype.ID() != rightDtype.ID() {
		debug.Warnf("warning: comparing different types of columns: %v | %v", leftDtype.Name(), rightDtype.Name())
		return false
	}

	// Let's use the stuff we already have to do all columns
	it := iterator.NewStepIteratorForColumns([]array.Column{*left, *right})
	defer it.Release()

	for it.Next() {
		stepValue := it.Values()
		var elTPrev Element
		for i := range stepValue.Values {
			elT := StepValueElementAt(stepValue, i)
			if elTPrev == nil {
				elTPrev = elT
				continue
			}
			eq, err := elT.EqStrict(elTPrev)
			if err != nil {
				debug.Warnf("warning: bullseye/dataframe#compareColumns: %v\n", err)
				// types must not be equal
				return false
			}
			if !eq {
				return false
			}
		}
	}

	return true
}

func buildSchema(cols []array.Column) *arrow.Schema {
	fields := make([]arrow.Field, 0, len(cols))
	for i := range cols {
		fields = append(fields, cols[i].Field())
	}
	return arrow.NewSchema(fields, nil)
}

// columnLen returns the number of rows in the Column.
// Because Arrow chunks arrays, you may encounter an overflow if
// there are MaxInt64 rows, i.e. 9223372036854775807.
func columnLen(col array.Column) int64 {
	var length int64
	for _, chunk := range col.Data().Chunks() {
		// Keep our own counters instead of Chunked's
		length += int64(chunk.Len())
	}
	return length
}
