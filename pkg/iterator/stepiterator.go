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

package iterator

import (
	"sync/atomic"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/gomem/gomem/internal/debug"
)

// StepValue holds the value for a given step.
type StepValue struct {
	Values     []interface{}
	ValuesJSON []interface{}
	Exists     []bool
	Dtypes     []arrow.DataType
}

// Value returns the value at index i and the data type for that value.
func (sv StepValue) Value(i int) (interface{}, arrow.DataType) {
	return sv.Values[i], sv.Dtypes[i]
}

// StepIterator iterates over multiple iterators in step.
type StepIterator interface {
	Values() *StepValue
	ValuesJSON() (*StepValue, error)
	Next() bool
	Retain()
	Release()
}

// stepIterator has a max number of elements it
// can iterator over that must fit into uint64
// which I doubt anyone is going to go over.
type stepIterator struct {
	refCount  int64
	iterators []ValueIterator
	index     uint64
	stepValue *StepValue
	dtypes    []arrow.DataType
}

// NewStepIteratorForColumns creates a new StepIterator given a slice of columns.
func NewStepIteratorForColumns(cols []array.Column) StepIterator {
	itrs := make([]ValueIterator, 0, len(cols))
	dtypes := make([]arrow.DataType, 0, len(cols))
	for i := range cols {
		itrs = append(itrs, NewValueIterator(&cols[i]))
		dtypes = append(dtypes, cols[i].DataType())
	}
	// NewStepIterator will retain the value iterators refs
	// so we need to remove our ref to them.
	for i := range itrs {
		defer itrs[i].Release()
	}
	return NewStepIterator(dtypes, itrs...)
}

// NewStepIterator creates a new StepIterator given a bunch of ValueIterators.
func NewStepIterator(dtypes []arrow.DataType, iterators ...ValueIterator) StepIterator {
	for i := range iterators {
		iterators[i].Retain()
	}
	return &stepIterator{
		refCount:  1,
		iterators: iterators,
		index:     0,
		dtypes:    dtypes,
	}
}

// Values returns the values in the current step as a StepValue.
func (s *stepIterator) Values() *StepValue {
	if s.stepValue.Values != nil {
		return s.stepValue
	}

	s.stepValue.Values = make([]interface{}, len(s.iterators))
	for i, iterator := range s.iterators {
		if s.stepValue.Exists[i] {
			s.stepValue.Values[i] = iterator.ValueInterface()
		} else {
			s.stepValue.Values[i] = nil
		}
	}

	return s.stepValue
}

// ValuesJSON returns the json values in the current step as a StepValue.
func (s *stepIterator) ValuesJSON() (*StepValue, error) {
	if s.stepValue.ValuesJSON != nil {
		return s.stepValue, nil
	}

	var err error
	s.stepValue.ValuesJSON = make([]interface{}, len(s.iterators))
	for i, iterator := range s.iterators {
		if s.stepValue.Exists[i] {
			s.stepValue.ValuesJSON[i], err = iterator.ValueAsJSON()
			if err != nil {
				return nil, err
			}
		} else {
			s.stepValue.ValuesJSON[i] = nil
		}
	}
	return s.stepValue, nil
}

// Next returns false when there are no more rows in any iterator.
func (s *stepIterator) Next() bool {
	// build the step values
	step := &StepValue{
		Values: nil,
		Exists: make([]bool, len(s.iterators)),
		Dtypes: s.dtypes,
	}

	next := false
	for i, iterator := range s.iterators {
		exists := iterator.Next()
		next = exists || next
		step.Exists[i] = exists
	}

	s.stepValue = step
	return next
}

func (s *stepIterator) Retain() {
	atomic.AddInt64(&s.refCount, 1)
}

func (s *stepIterator) Release() {
	refs := atomic.AddInt64(&s.refCount, -1)
	debug.Assert(refs >= 0, "too many releases")
	if refs == 0 {
		for i := range s.iterators {
			s.iterators[i].Release()
		}
		s.iterators = nil
	}
}
