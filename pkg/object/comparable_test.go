package object

import (
	"errors"
	"testing"
)

func TestComparableEq(t *testing.T) {
	cases := []struct {
		left  Object
		right Object
		err   error
		want  bool
	}{
		{Int64(1), Int64(1), nil, true},
		{Int64(1), Int64(2), nil, false},
		{nil, nil, nil, true},
		{nil, Int64(1), nil, false},
		{Int64(1), nil, nil, false},
		{Int64(1).ToBoolean(), Int32(2).ToBoolean(), nil, true},
		{Int64(0).ToBoolean(), Int32(0).ToBoolean(), nil, true},
		{Int64(0).ToBoolean(), Int32(1).ToBoolean(), nil, false},
	}
	for _, c := range cases {
		eq, err := Eq(c.left, c.right)
		if err != c.err {
			t.Errorf("wrong value for error:\ngot=%v\nwant=%v", err, c.err)
		}

		if got, want := eq, Boolean(c.want); got != want {
			t.Errorf("\ngot=%v\nwant=%v", got, want)
		}
	}
}

func TestComparableNeq(t *testing.T) {
	cases := []struct {
		left  Object
		right Object
		err   error
		want  bool
	}{
		{Int64(1), Int64(1), nil, false},
		{Int64(1), Int64(2), nil, true},
		{nil, nil, nil, false},
		{nil, Int64(1), nil, true},
		{Int64(1), nil, nil, true},
	}
	for _, c := range cases {
		eq, err := Neq(c.left, c.right)
		if err != c.err {
			t.Errorf("wrong value for error:\ngot=%v\nwant=%v", err, c.err)
		}

		if got, want := eq, Boolean(c.want); got != want {
			t.Errorf("\ngot=%v\nwant=%v", got, want)
		}
	}
}

func TestComparableLess(t *testing.T) {
	cases := []struct {
		left  Object
		right Object
		err   error
		want  bool
	}{
		{Int64(1), Int64(1), nil, false},
		{Int64(1), Int64(2), nil, true},
		{Int64(2), Int64(1), nil, false},
		{nil, nil, errors.New("less than not defined on nil"), false},
		{nil, Int64(1), errors.New("less than not defined on nil"), false},
		{Int64(1), nil, errors.New("less than not defined on nil"), false},
	}
	for _, c := range cases {
		eq, err := Less(c.left, c.right)
		if err != c.err && err.Error() != c.err.Error() {
			t.Errorf("wrong value for error:\ngot=%v\nwant=%v", err, c.err)
		}

		if got, want := eq, Boolean(c.want); got != want {
			t.Errorf("\ngot=%v\nwant=%v", got, want)
		}
	}
}

func TestComparableLessEq(t *testing.T) {
	cases := []struct {
		left  Object
		right Object
		err   error
		want  bool
	}{
		{Int64(1), Int64(1), nil, true},
		{Int64(1), Int64(2), nil, true},
		{Int64(2), Int64(1), nil, false},
		{nil, nil, errors.New("less than or equal to not defined on nil"), false},
		{nil, Int64(1), errors.New("less than or equal to not defined on nil"), false},
		{Int64(1), nil, errors.New("less than or equal to not defined on nil"), false},
	}
	for _, c := range cases {
		eq, err := LessEq(c.left, c.right)
		if err != c.err && err.Error() != c.err.Error() {
			t.Errorf("wrong value for error:\ngot=%v\nwant=%v", err, c.err)
		}

		if got, want := eq, Boolean(c.want); got != want {
			t.Errorf("\ngot=%v\nwant=%v", got, want)
		}
	}
}

func TestComparableGreater(t *testing.T) {
	cases := []struct {
		left  Object
		right Object
		err   error
		want  bool
	}{
		{Int64(1), Int64(1), nil, false},
		{Int64(1), Int64(2), nil, false},
		{Int64(2), Int64(1), nil, true},
		{nil, nil, errors.New("greater than not defined on nil"), false},
		{nil, Int64(1), errors.New("greater than not defined on nil"), false},
		{Int64(1), nil, errors.New("greater than not defined on nil"), false},
	}
	for _, c := range cases {
		eq, err := Greater(c.left, c.right)
		if err != c.err && err.Error() != c.err.Error() {
			t.Errorf("wrong value for error:\ngot=%v\nwant=%v", err, c.err)
		}

		if got, want := eq, Boolean(c.want); got != want {
			t.Errorf("\ngot=%v\nwant=%v", got, want)
		}
	}
}

func TestComparableGreaterEq(t *testing.T) {
	cases := []struct {
		left  Object
		right Object
		err   error
		want  bool
	}{
		{Int64(1), Int64(1), nil, true},
		{Int64(1), Int64(2), nil, false},
		{Int64(2), Int64(1), nil, true},
		{nil, nil, errors.New("greater than or equal to not defined on nil"), false},
		{nil, Int64(1), errors.New("greater than or equal to not defined on nil"), false},
		{Int64(1), nil, errors.New("greater than or equal to not defined on nil"), false},
	}
	for _, c := range cases {
		eq, err := GreaterEq(c.left, c.right)
		if err != c.err && err.Error() != c.err.Error() {
			t.Errorf("wrong value for error:\ngot=%v\nwant=%v", err, c.err)
		}

		if got, want := eq, Boolean(c.want); got != want {
			t.Errorf("\ngot=%v\nwant=%v", got, want)
		}
	}
}
