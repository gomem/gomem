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
	"errors"
	"fmt"
)

const (
	STRING_VALUE = "(null)"
)

// CastToNull takes any Object type and converts it to Null
func CastToNull(v interface{}) (Null, bool) {
	return Null{}, true
}

// NewNull creates a new Null object
// from the given value provided as v.
func NewNull() Null {
	return Null{}
}

// Null has logic to apply to this type.
type Null struct{}

// Value returns the underlying value in it's native type.
func (e Null) Value() interface{} {
	return nil
}

// compare takes the left and right objects and applies the comparator function to them.
func (e Null) compare(r Object, f func(Null, Null) Boolean) (Boolean, error) {
	if r == nil {
		return false, nil
	}

	var right Null
	switch t := r.(type) {
	case Null:
		right = t
	case *Null:
		right = *t
	default:
		return false, fmt.Errorf("cannot cast %v to Null", r)
	}

	return f(e, right), nil
}

// Comparation methods

// Eq returns true if the left Null is equal to the right Null.
// Since they are both Null types holding no values they must be equal
// if r Object is in fact a Null Object type.
func (e Null) Eq(r Object) (Boolean, error) {
	return e.compare(r, func(left, right Null) Boolean {
		// Since left and right are both Null types they must be equal.
		return Boolean(true)
	})
}

// Neq returns true if the left Null
// is not equal to the right Null.
func (e Null) Neq(r Object) (Boolean, error) {
	v, ok := e.Eq(r)
	return !v, ok
}

// Less returns true if the left Null
// is less than the right Null.
func (e Null) Less(r Object) (Boolean, error) {
	return false, errors.New("less than not defined on Null")
}

// LessEq returns true if the left Null
// is less than or equal to the right Null.
func (e Null) LessEq(r Object) (Boolean, error) {
	return false, errors.New("less than or equal to not defined on Null")
}

// Greater returns true if the left Null
// is greter than the right Null.
func (e Null) Greater(r Object) (Boolean, error) {
	return false, errors.New("greater than not defined on Null")
}

// GreaterEq returns true if the left Null
// is greter than or equal to the right Null.
func (e Null) GreaterEq(r Object) (Boolean, error) {
	return false, errors.New("greater than or equal to not defined on Null")
}

// Accessor/conversion methods

// String prints the value of this element as a string.
func (e Null) String() string {
	return STRING_VALUE
}

// ToBoolean always returns false for the Null type.
func (e Null) ToBoolean() Boolean {
	return Boolean(false)
}

var (
	_ Object = (*Null)(nil)
)
