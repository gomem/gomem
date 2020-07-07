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

/*
Package dataframe provides an implementation of a DataFrame using Apache Arrow.

Basics

The DataFrame is an immutable heterogeneous tabular data structure with labeled columns.
It stores it's raw bytes using a provided Arrow Allocator by using the fundamental data
structure of Array (columns), which holds a sequence of values of the same type. An array
consists of memory holding the data and an additional validity bitmap that indicates if
the corresponding entry in the array is valid (not null).

Any DataFrames created should be released using Release() to decrement the reference
and free up the memory managed by the Arrow implementation.

Getting Started

Look in dataframe_tests.go for examples to get started.

*/
package dataframe
