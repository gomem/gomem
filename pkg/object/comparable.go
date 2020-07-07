package object

import "errors"

type Comparable interface {
	// Compare methods
	// Eq returns true if the left Object is equal to the right Object.
	Eq(Object) (Boolean, error)
	// Neq returns true if the left Object is not equal to the right Object.
	Neq(Object) (Boolean, error)
	// Less returns true if the left Object is less than the right Object.
	Less(Object) (Boolean, error)
	// LessEq returns true if the left Object is less than or equal to the right Object.
	LessEq(Object) (Boolean, error)
	// Greater returns true if the left Object is greter than the right Object.
	Greater(Object) (Boolean, error)
	// GreaterEq returns true if the left Object is greter than or equal to the right Object.
	GreaterEq(Object) (Boolean, error)
	// ToBoolean returns true when the value of this Object is not false.
	ToBoolean() (value Boolean)
	// IsEmpty returns true when the value of this object is equal to the
	// value of this type of Object in it's default state.
	// IsEmpty() bool
}

// These functions should be used when the left Object could be nil.
// They check if either is nil before calling the respective comparator method on the Object.

// Eq returns true if the left Object is equal to the right Object.
// If either is nil Eq returns false.
// Comparing different types is also an error.
func Eq(left Object, right Object) (Boolean, error) {
	leftIsNil := IsNil(left)
	rightIsNil := IsNil(right)
	// If both are nil they are equal.
	if leftIsNil && rightIsNil {
		return Boolean(true), nil
	}
	// If one is nil and the other isn't they are not equal
	if leftIsNil != rightIsNil {
		return Boolean(false), nil
	}
	return left.Eq(right)
}

// Neq returns true if the left Object is not equal to the right Object.
// If either is nil Neq returns true.
// Comparing different types is also an error.
func Neq(left Object, right Object) (Boolean, error) {
	leftIsNil := IsNil(left)
	rightIsNil := IsNil(right)
	// If both are nil they are equal.
	if leftIsNil && rightIsNil {
		return Boolean(false), nil
	}
	// If one is nil and the other isn't they are not equal
	if leftIsNil != rightIsNil {
		return Boolean(true), nil
	}
	return left.Neq(right)
}

// Less returns true if the left Object is less than the right Object.
// If either is nil Less returns false and an error.
// Comparing different types is also an error.
func Less(left Object, right Object) (Boolean, error) {
	// If one is nil and the other isn't they cannot be compared.
	if IsNil(left) || IsNil(right) {
		return Boolean(false), errors.New("less than not defined on nil")
	}
	return left.Less(right)
}

// LessEq returns true if the left Object is less than or equal to the right Object.
// If either is nil LessEq returns false and an error.
// Comparing different types is also an error.
func LessEq(left Object, right Object) (Boolean, error) {
	// If one is nil and the other isn't they cannot be compared.
	if IsNil(left) || IsNil(right) {
		return Boolean(false), errors.New("less than or equal to not defined on nil")
	}
	return left.LessEq(right)
}

// Greater returns true if the left Object is greter than the right Object.
// If either is nil Greater returns false and an error.
// Comparing different types is also an error.
func Greater(left Object, right Object) (Boolean, error) {
	// If one is nil and the other isn't they cannot be compared.
	if IsNil(left) || IsNil(right) {
		return Boolean(false), errors.New("greater than not defined on nil")
	}
	return left.Greater(right)
}

// GreaterEq returns true if the left Object is greter than or equal to the right Object.
// If either is nil GreaterEq returns false and an error.
// Comparing different types is also an error.
func GreaterEq(left Object, right Object) (Boolean, error) {
	// If one is nil and the other isn't they cannot be compared.
	if IsNil(left) || IsNil(right) {
		return Boolean(false), errors.New("greater than or equal to not defined on nil")
	}
	return left.GreaterEq(right)
}
