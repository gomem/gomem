package object

import (
	"errors"
	"testing"
)

func TestNullEq(t *testing.T) {
	cases := []struct {
		left  Object
		right Object
		err   error
		want  bool
	}{
		{NewNull(), NewNull(), nil, true},
		{&Null{}, &Null{}, nil, true},
	}
	for _, c := range cases {
		eq, err := c.left.Eq(c.right)
		if err != c.err {
			t.Errorf("wrong value for error:\ngot=%v\nwant=%v", err, c.err)
		}

		if got, want := eq, Boolean(c.want); got != want {
			t.Errorf("\ngot=%v\nwant=%v", got, want)
		}
	}
}

func TestNullNeq(t *testing.T) {
	cases := []struct {
		left  Object
		right Object
		err   error
		want  bool
	}{
		{NewNull(), NewNull(), nil, false},
		{&Null{}, &Null{}, nil, false},
	}
	for _, c := range cases {
		eq, err := c.left.Neq(c.right)
		if err != c.err {
			t.Errorf("wrong value for error:\ngot=%v\nwant=%v", err, c.err)
		}

		if got, want := eq, Boolean(c.want); got != want {
			t.Errorf("\ngot=%v\nwant=%v", got, want)
		}
	}
}

func TestNullLess(t *testing.T) {
	cases := []struct {
		left  Object
		right Object
		err   error
		want  bool
	}{
		{NewNull(), NewNull(), errors.New("less than not defined on Null"), false},
		{&Null{}, &Null{}, errors.New("less than not defined on Null"), false},
	}
	for _, c := range cases {
		eq, err := c.left.Less(c.right)
		if err.Error() != c.err.Error() {
			t.Errorf("wrong value for error:\ngot=%v\nwant=%v", err, c.err)
		}

		if got, want := eq, Boolean(c.want); got != want {
			t.Errorf("\ngot=%v\nwant=%v", got, want)
		}
	}
}

func TestNullLessEq(t *testing.T) {
	cases := []struct {
		left  Object
		right Object
		err   error
		want  bool
	}{
		{NewNull(), NewNull(), errors.New("less than or equal to not defined on Null"), false},
		{&Null{}, &Null{}, errors.New("less than or equal to not defined on Null"), false},
	}
	for _, c := range cases {
		eq, err := c.left.LessEq(c.right)
		if err.Error() != c.err.Error() {
			t.Errorf("wrong value for error:\ngot=%v\nwant=%v", err, c.err)
		}

		if got, want := eq, Boolean(c.want); got != want {
			t.Errorf("\ngot=%v\nwant=%v", got, want)
		}
	}
}

func TestNullGreater(t *testing.T) {
	cases := []struct {
		left  Object
		right Object
		err   error
		want  bool
	}{
		{NewNull(), NewNull(), errors.New("greater than not defined on Null"), false},
		{&Null{}, &Null{}, errors.New("greater than not defined on Null"), false},
	}
	for _, c := range cases {
		eq, err := c.left.Greater(c.right)
		if err.Error() != c.err.Error() {
			t.Errorf("wrong value for error:\ngot=%v\nwant=%v", err, c.err)
		}

		if got, want := eq, Boolean(c.want); got != want {
			t.Errorf("\ngot=%v\nwant=%v", got, want)
		}
	}
}

func TestNullGreaterEq(t *testing.T) {
	cases := []struct {
		left  Object
		right Object
		err   error
		want  bool
	}{
		{NewNull(), NewNull(), errors.New("greater than or equal to not defined on Null"), false},
		{&Null{}, &Null{}, errors.New("greater than or equal to not defined on Null"), false},
	}
	for _, c := range cases {
		eq, err := c.left.GreaterEq(c.right)
		if err.Error() != c.err.Error() {
			t.Errorf("wrong value for error:\ngot=%v\nwant=%v", err, c.err)
		}

		if got, want := eq, Boolean(c.want); got != want {
			t.Errorf("\ngot=%v\nwant=%v", got, want)
		}
	}
}

func TestNullString(t *testing.T) {
	null := NewNull()
	if got, want := null.String(), "(null)"; got != want {
		t.Errorf("\ngot=%v\nwant=%v", got, want)
	}
}

func TestNullToBoolean(t *testing.T) {
	cases := []struct {
		Obj  Null
		Want bool
	}{
		{Null{}, false},
		{NewNull(), false},
	}
	for _, c := range cases {
		if got, want := c.Obj.ToBoolean(), Boolean(c.Want); got != want {
			t.Errorf("\ngot=%v\nwant=%v", got, want)
		}
	}
}

func TestNullValue(t *testing.T) {
	null := NewNull()
	got := null.Value()
	if got != nil {
		t.Errorf("\ngot=%v\nwant=%v", got, nil)
	}
}
