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

package object

import (
	"fmt"
)

func (e Boolean) validateForCompare(r Object, f func(Boolean, Boolean) Boolean) (Boolean, error) {
	if r == nil {
		return Boolean(false), nil
	}

	var right Boolean
	switch t := r.(type) {
	case Boolean:
		right = t
	case *Boolean:
		right = *t
	default:
		return false, fmt.Errorf("cannot cast %v to Boolean", r)
	}

	return f(e, right), nil
}

// Less returns true if the left Boolean
// is less than the right Boolean.
func (e Boolean) Less(r Object) (Boolean, error) {
	return e.validateForCompare(r, func(left, right Boolean) Boolean {
		return Boolean(left.ToInt8() < right.ToInt8())
	})
}

// LessEq returns true if the left Boolean
// is less than or equal to the right Boolean.
func (e Boolean) LessEq(r Object) (Boolean, error) {
	return e.validateForCompare(r, func(left, right Boolean) Boolean {
		return Boolean(left.ToInt8() <= right.ToInt8())
	})
}

// Greater returns true if the left Boolean
// is greter than the right Boolean.
func (e Boolean) Greater(r Object) (Boolean, error) {
	return e.validateForCompare(r, func(left, right Boolean) Boolean {
		return Boolean(left.ToInt8() > right.ToInt8())
	})
}

// GreaterEq returns true if the left Boolean
// is greter than or equal to the right Boolean.
func (e Boolean) GreaterEq(r Object) (Boolean, error) {
	return e.validateForCompare(r, func(left, right Boolean) Boolean {
		return Boolean(left.ToInt8() >= right.ToInt8())
	})
}
