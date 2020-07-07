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
Package iterator provides iterators for chunks and values.

Since Arrow can store chunks larger than the max int64 (9223372036854775807) due to how it
stores chunks, it's best to use iterators to iterate over chunks and their values.

There are generic ChunkIterator and ValueIterator implementations as well as specific
generated Arrow types for each of them, i.e. Float64ChunkIterator and Float64ValueIterator.

*/
package iterator
