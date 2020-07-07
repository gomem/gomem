package object

import "testing"

func Test_toI64(t *testing.T) {
	cases := []struct {
		in   Decimal128
		want int64
	}{
		// https://github.com/apache/arrow/blob/eb8080e2a5946d7d279982be77be3eb138b04c53/cpp/src/arrow/util/decimal_test.cc#L424
		{NewDecimal128FromInt64(1234), 1234},
		{NewDecimal128FromInt64(-1234), -1234},
	}

	for _, c := range cases {
		got := c.in.toI64()
		if got != c.want {
			t.Errorf("%+v\ngot=%v\nwant=%v", c, got, c.want)
		}
	}
}

func Test_toU64(t *testing.T) {
	cases := []struct {
		in   Decimal128
		want uint64
	}{
		// https://github.com/apache/arrow/blob/eb8080e2a5946d7d279982be77be3eb138b04c53/cpp/src/arrow/util/decimal_test.cc#L424
		{NewDecimal128FromInt64(1234), 1234},
	}

	for _, c := range cases {
		got := c.in.toU64()
		if got != c.want {
			t.Errorf("%+v\ngot=%v\nwant=%v", c, got, c.want)
		}
	}
}
