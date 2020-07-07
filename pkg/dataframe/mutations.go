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
	"fmt"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/gomem/gomem/pkg/iterator"
	"github.com/gomem/gomem/pkg/smartbuilder"
)

// Mutator is a type that has some standard mutations.
type Mutator struct {
	// Almost all mutations will require setting up new memory as they create new a DataFrame.
	// So we need to provide the ability to set the Allocator.
	mem memory.Allocator
}

// NewMutator creates a new mutator.
func NewMutator(mem memory.Allocator) *Mutator {
	return &Mutator{
		mem: mem,
	}
}

// MutationFunc is a function that mutates an existing DataFrame and returns a new DataFrame or an error.
type MutationFunc func(*DataFrame) (*DataFrame, error)

// Select the given DataFrame columns by name.
func (m *Mutator) Select(names ...string) MutationFunc {
	return func(df *DataFrame) (*DataFrame, error) {
		cols := df.SelectColumns(names...)
		return NewDataFrameFromShape(m.mem, cols, df.NumRows())
	}
}

// Drop the given DataFrame columns by name.
func (m *Mutator) Drop(names ...string) MutationFunc {
	return func(df *DataFrame) (*DataFrame, error) {
		cols := df.RejectColumns(names...)
		return NewDataFrameFromShape(m.mem, cols, df.NumRows())
	}
}

// Slice creates a new DataFrame consisting of rows[beg:end].
func (m *Mutator) Slice(beg, end int64) MutationFunc {
	return func(df *DataFrame) (*DataFrame, error) {
		if end > df.NumRows() || beg > end {
			return nil, fmt.Errorf("mutation: index out of range")
		}

		dfCols := df.Columns()

		cols := make([]array.Column, len(dfCols))
		for i, col := range dfCols {
			cols[i] = *col.NewSlice(beg, end)
		}

		defer func() {
			for i := range cols {
				cols[i].Release()
			}
		}()

		rows := end - beg
		return NewDataFrameFromShape(m.mem, cols, rows)
	}
}

// leftJoinConfig are the config params for LeftJoin.
type leftJoinConfig struct {
	lsuffix string
	rsuffix string
}

// newLeftJoinConfig creates a new config using options and validates it.
func newLeftJoinConfig(opts ...Option) (*leftJoinConfig, error) {
	cfg := defaultLeftJoinConfig()
	for _, opt := range opts {
		if err := opt(cfg); err != nil {
			return cfg, err
		}
	}
	err := cfg.validate()
	return cfg, err
}

func (c *leftJoinConfig) validate() error {
	if c.lsuffix == c.rsuffix {
		return fmt.Errorf("lsuffix (%s) cannot be the same as rsuffix (%s)", c.lsuffix, c.rsuffix)
	}
	return nil
}

// defaultLeftJoinConfig returns the default defaultLeftJoinConfig.
func defaultLeftJoinConfig() *leftJoinConfig {
	return &leftJoinConfig{
		lsuffix: "_0",
		rsuffix: "_1",
	}
}

// WithLsuffix configures a right or left join to use the provided left suffix.
func WithLsuffix(lsuffix string) Option {
	return func(p interface{}) error {
		o, ok := p.(*leftJoinConfig)
		if !ok {
			return fmt.Errorf("cannot apply WithLsuffix to: %T", p)
		}
		o.lsuffix = lsuffix
		return nil
	}
}

// WithRsuffix configures a right or left join to use the provided left suffix.
func WithRsuffix(rsuffix string) Option {
	return func(p interface{}) error {
		o, ok := p.(*leftJoinConfig)
		if !ok {
			return fmt.Errorf("cannot apply WithRsuffix to: %T", p)
		}
		o.rsuffix = rsuffix
		return nil
	}
}

// RightJoin returns a DataFrame containing the right join of two DataFrames.
// Acts like SQL in that nil elements are treated as unknown so nil != nil.
func (m *Mutator) RightJoin(rightDf *DataFrame, columnNames []string, opts ...Option) MutationFunc {
	// RightJoin is just a LeftJoin in reverse order.
	cfg, err := newLeftJoinConfig(opts...)
	if err == nil {
		// Swap lsuffix and rsuffix
		lsuffix := cfg.lsuffix
		cfg.lsuffix = cfg.rsuffix
		cfg.rsuffix = lsuffix
	}

	return func(leftDf *DataFrame) (*DataFrame, error) {
		if err != nil {
			return nil, err
		}

		// We swap leftDf and rightDf
		data, err := m.leftJoin(cfg, rightDf, leftDf, columnNames)
		if err != nil {
			return nil, err
		}
		defer data.Release()
		// return fn(rightDf)
		return data.buildDataFrame()
	}
}

// LeftJoin returns a DataFrame containing the left join of two DataFrames.
// Acts like SQL in that nil elements are treated as unknown so nil != nil.
func (m *Mutator) LeftJoin(rightDf *DataFrame, columnNames []string, opts ...Option) MutationFunc {
	cfg, err := newLeftJoinConfig(opts...)
	return func(leftDf *DataFrame) (*DataFrame, error) {
		if err != nil {
			return nil, err
		}

		data, err := m.leftJoin(cfg, leftDf, rightDf, columnNames)
		if err != nil {
			return nil, err
		}
		defer data.Release()
		return data.buildDataFrame()
	}
}

type joinFuncConfig struct {
	// Keep a ref to the mutator that created it
	mutator *Mutator

	matchingLeftColsLen    int
	matchingRightColsLen   int
	additionalLeftColsLen  int
	additionalRightColsLen int
	columnNames            []string
	leftColumns            []array.Column
	rightColumns           []array.Column
	schema                 *arrow.Schema
	recordBuilder          *array.RecordBuilder
	smartBuilder           *smartbuilder.SmartBuilder
}

// newJoinFuncConfig builds up all the data needed to do a join.
// TODO(nickpoorman): maybe rename leftJoinConfig if this is going to be used for other joins
func (m *Mutator) newJoinFuncConfig(cfg *leftJoinConfig, leftDf *DataFrame, rightDf *DataFrame, columnNames []string, forceNullable bool) (*joinFuncConfig, error) {
	jc := &joinFuncConfig{
		mutator:      m,
		columnNames:  columnNames,
		leftColumns:  make([]array.Column, 0, leftDf.NumCols()),
		rightColumns: make([]array.Column, 0, rightDf.NumCols()),
	}

	// Start by making sure that both DataFrames have the columns we are looking for.
	for _, name := range columnNames {
		leftColumn := leftDf.Column(name)
		if leftColumn == nil {
			return nil, fmt.Errorf("bullseye/mutations: column %s is not in left DataFrame: (%v)", name, leftDf.ColumnNames())
		}
		rightColumn := rightDf.Column(name)
		if rightColumn == nil {
			return nil, fmt.Errorf("bullseye/mutations: column %s is not in right DataFrame: (%v)", name, rightDf.ColumnNames())
		}

		jc.leftColumns = append(jc.leftColumns, *leftColumn)
		jc.rightColumns = append(jc.rightColumns, *rightColumn)
	}
	// Keep track of the number of matching left and right columns. (They should be the same number)
	jc.matchingLeftColsLen = len(jc.leftColumns)
	jc.matchingRightColsLen = len(jc.rightColumns)

	// We will end up needing to iterate over the columns for left in step so join them back together.
	jc.leftColumns = append(jc.leftColumns, leftDf.RejectColumns(columnNames...)...)
	jc.rightColumns = append(jc.rightColumns, rightDf.RejectColumns(columnNames...)...)

	// Keep track of the lengths. Now that we have appended the other columns.
	jc.additionalLeftColsLen = len(jc.leftColumns) - jc.matchingLeftColsLen
	jc.additionalRightColsLen = len(jc.rightColumns) - jc.matchingRightColsLen

	// get all the fields that make up the schema
	fields := make([]arrow.Field, 0, jc.matchingLeftColsLen+jc.additionalLeftColsLen+jc.additionalRightColsLen)
	for i := 0; i < len(jc.leftColumns); i++ {
		fields = append(fields, jc.leftColumns[i].Field())
	}
	for i := jc.matchingRightColsLen; i < len(jc.rightColumns); i++ {
		fcopy := jc.rightColumns[i].Field()
		if forceNullable {
			// This column's values must be nullable since there may not be any matches.
			fcopy.Nullable = true
		}
		// If there are any existing fields that have this name we must change the names.
		name := fcopy.Name
		// Start at the end of the matching ones because those clearly wont have a conflict.
		for i := jc.matchingLeftColsLen; i < len(jc.leftColumns); i++ {
			if fields[i].Name == name {
				fields[i].Name = fmt.Sprintf("%s%s", name, cfg.lsuffix)
				fcopy.Name = fmt.Sprintf("%s%s", name, cfg.rsuffix)
				break
			}
		}

		fields = append(fields, fcopy)
	}

	jc.schema = arrow.NewSchema(fields, nil)
	jc.recordBuilder = array.NewRecordBuilder(m.mem, jc.schema)
	jc.smartBuilder = smartbuilder.NewSmartBuilder(jc.recordBuilder)

	return jc, nil
}

func (jc *joinFuncConfig) Release() {
	jc.recordBuilder.Release()
}

func (jc *joinFuncConfig) buildDataFrame() (*DataFrame, error) {
	rec := jc.recordBuilder.NewRecord()
	defer rec.Release()
	return NewDataFrame(jc.mutator.mem, jc.schema, rec.Columns())
}

// This leftJoin implementation is shared by both LeftJoin and RightJoin.
// Acts like SQL in that nil elements are treated as unknown so nil != nil.
func (m *Mutator) leftJoin(cfg *leftJoinConfig, leftDf *DataFrame, rightDf *DataFrame, columnNames []string) (*joinFuncConfig, error) {
	data, err := m.newJoinFuncConfig(cfg, leftDf, rightDf, columnNames, true)
	if err != nil {
		return nil, err
	}

	sharedLeftJoinLogic(data, func(appendEmptyRow bool, leftStepValues *iterator.StepValue) {
		if appendEmptyRow {
			// If nothing matched then we append the row once with nil for additional right columns.
			cIdx := 0

			// Add all the values from left columns
			for i := range leftStepValues.Values {
				data.smartBuilder.Append(cIdx, leftStepValues.Values[i])
				cIdx++
			}

			for i := 0; i < data.additionalRightColsLen; i++ {
				// cIdx is the offset to the start of the additionalRightCols in smartBuilder
				data.smartBuilder.Append(cIdx+i, nil)
			}
		}
	})

	return data, nil
}

// Acts like SQL in that nil elements are treated as unknown so nil != nil.
func sharedLeftJoinLogic(data *joinFuncConfig, iterationEndFunc func(bool, *iterator.StepValue)) {
	// What I want here is a step iterator for the matchingLeftCols.
	leftMatchingIterator := iterator.NewStepIteratorForColumns(data.leftColumns)
	defer leftMatchingIterator.Release()
	for leftMatchingIterator.Next() { // Iterate through every row in the left df.
		leftStepValues := leftMatchingIterator.Values()
		// If we don't find a match, we'll need to append an empty row.
		appendEmptyRow := true

		func() {
			// What I want here is a step iterator for the matchingRightCols.
			rightMatchingIterator := iterator.NewStepIteratorForColumns(data.rightColumns)
			defer rightMatchingIterator.Release()
			for rightMatchingIterator.Next() { // Iterate through every row in the right df.
				rightStepValues := rightMatchingIterator.Values()
				match := true

				// For each matching column,
				// check if the row on the left,
				// matches with the rows on the right.
				for columnIndex := range data.columnNames {
					match = match && stepValueEqAt(leftStepValues, rightStepValues, columnIndex)
				}

				if match {
					// For each match, we append a new row with the
					// left columns values and the additional right column values.
					appendEmptyRow = false

					// Keep track of the number of columns we need to offset by so we know what index we are on.
					cIdx := 0

					// Add all the values from left columns
					for i := range leftStepValues.Values {
						data.smartBuilder.Append(cIdx, leftStepValues.Values[i])
						cIdx++
					}

					// Do the dance we did above and append the elements to each column for additionalRightCols.
					for i := data.matchingRightColsLen; i < len(data.rightColumns); i++ {
						value := rightStepValues.Values[i]
						data.smartBuilder.Append(cIdx, value)
						cIdx++
					}
				}
			}
		}()
		iterationEndFunc(appendEmptyRow, leftStepValues)
	}
}

// InnerJoin returns a DataFrame containing the inner join of two DataFrames.
// Acts like SQL in that nil elements are treated as unknown so nil != nil.
func (m *Mutator) InnerJoin(rightDf *DataFrame, columnNames []string, opts ...Option) MutationFunc {
	cfg, err := newLeftJoinConfig(opts...)
	return func(leftDf *DataFrame) (*DataFrame, error) {
		if err != nil {
			return nil, err
		}

		data, err := m.newJoinFuncConfig(cfg, leftDf, rightDf, columnNames, false)
		if err != nil {
			return nil, err
		}
		defer data.Release()

		// InnerJoin is basically LeftJoin without appending nulls in iterationEndFunc so we stub that callback.
		sharedLeftJoinLogic(data, func(bool, *iterator.StepValue) {})

		return data.buildDataFrame()
	}
}

// OuterJoin returns a DataFrame containing the outer join of two DataFrames.
// Use union of keys from both frames, similar to a SQL full outer join.
// Acts like SQL in that nil elements are treated as unknown so nil != nil.
func (m *Mutator) OuterJoin(rightDf *DataFrame, columnNames []string, opts ...Option) MutationFunc {
	cfg, err := newLeftJoinConfig(opts...)
	return func(leftDf *DataFrame) (*DataFrame, error) {
		if err != nil {
			return nil, err
		}

		data, err := m.leftJoin(cfg, leftDf, rightDf, columnNames)
		if err != nil {
			return nil, err
		}
		defer data.Release()

		// Now we iterate over the right first.
		rightIterator := iterator.NewStepIteratorForColumns(data.rightColumns)
		defer rightIterator.Release()
		for rightIterator.Next() { // Iterate through every row in the right df.
			rightStepValues := rightIterator.Values()
			// If we don't find a match, we'll need to append an empty row.

			if !outerJoinAnyRowsMatch(rightStepValues, data) {
				// Keep track of the number of columns we need to offset by so we know what index we are on.
				cIdx := 0

				// Add all the values from right matching columns
				for i := 0; i < data.matchingRightColsLen; i++ {
					value := rightStepValues.Values[i]
					data.smartBuilder.Append(cIdx, value)
					cIdx++
				}

				// Add nil for not matching left columns
				for i := 0; i < data.additionalLeftColsLen; i++ {
					data.smartBuilder.Append(cIdx, nil)
					cIdx++
				}

				// Add the additional values from the right.
				for i := data.matchingRightColsLen; i < data.matchingRightColsLen+data.additionalRightColsLen; i++ {
					value := rightStepValues.Values[i]
					data.smartBuilder.Append(cIdx, value)
					cIdx++
				}
			}
		}

		return data.buildDataFrame()
	}
}

func outerJoinAnyRowsMatch(rightStepValues *iterator.StepValue, data *joinFuncConfig) bool {
	leftIterator := iterator.NewStepIteratorForColumns(data.leftColumns)
	defer leftIterator.Release()
	for leftIterator.Next() { // Iterate through every row in the left df.
		leftStepValues := leftIterator.Values()
		match := true

		// For each matching column,
		// check if the row on the left,
		// matches with the rows on the right.
		for columnIndex := range data.columnNames {
			match = match && stepValueEqAt(leftStepValues, rightStepValues, columnIndex)
		}

		if match {
			return true
		}
	}

	return false
}

// CrossJoin returns a DataFrame containing the cross join of two DataFrames.
func (m *Mutator) CrossJoin(rightDf *DataFrame, opts ...Option) MutationFunc {
	cfg, err := newLeftJoinConfig(opts...)
	return func(leftDf *DataFrame) (*DataFrame, error) {
		if err != nil {
			return nil, err
		}

		data, err := m.newJoinFuncConfig(cfg, leftDf, rightDf, nil, false)
		if err != nil {
			return nil, err
		}
		defer data.Release()

		leftMatchingIterator := iterator.NewStepIteratorForColumns(data.leftColumns)
		defer leftMatchingIterator.Release()
		for leftMatchingIterator.Next() { // Iterate through every row in the left df.
			leftStepValues := leftMatchingIterator.Values()

			func() {
				rightMatchingIterator := iterator.NewStepIteratorForColumns(data.rightColumns)
				defer rightMatchingIterator.Release()
				for rightMatchingIterator.Next() { // Iterate through every row in the right df.
					rightStepValues := rightMatchingIterator.Values()

					cIdx := 0

					// Add all columns from both frames.
					for i := range leftStepValues.Values {
						data.smartBuilder.Append(cIdx, leftStepValues.Values[i])
						cIdx++
					}
					for i := range rightStepValues.Values {
						data.smartBuilder.Append(cIdx, rightStepValues.Values[i])
						cIdx++
					}
				}
			}()
		}

		return data.buildDataFrame()
	}
}

func stepValueEqAt(left *iterator.StepValue, right *iterator.StepValue, i int) bool {
	lElem := StepValueElementAt(left, i)
	rElem := StepValueElementAt(right, i)

	v, err := lElem.Eq(rElem)
	if err != nil {
		panic(err)
	}

	return v
}
