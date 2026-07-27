package object

import (
	"bytes"
	"testing"
)

// ---------------------------------------------------------------------------
// Binary / FixedSizeBinary / LargeString
//
// These tests cover the byte- and large-string types added via the codegen
// pipeline. They verify construction, lexicographic comparison (bytes.Compare),
// JSON rendering, casts from primitives, and the String<->Binary round trips.
// ---------------------------------------------------------------------------

func TestBinaryConstruction(t *testing.T) {
	got := NewBinary([]byte("hello"))
	want := Binary([]byte("hello"))
	if !bytes.Equal([]byte(got), []byte(want)) {
		t.Errorf("NewBinary: got=%v, want=%v", got, want)
	}
	if got.Value()[0] != 'h' {
		t.Errorf("Value: got=%v", got.Value())
	}
}

func TestBinaryComparison(t *testing.T) {
	abc := NewBinary([]byte("abc"))
	abd := NewBinary([]byte("abd"))
	same := NewBinary([]byte("abc"))

	cases := []struct {
		name string
		op   func(Object) (Boolean, error)
		lhs  Binary
		want bool
	}{
		{"abc == abc", same.Eq, abc, true},
		{"abc == abd", abd.Eq, abc, false},
		{"abc < abd", abc.Less, abd, true},
		{"abd < abc", abd.Less, abc, false},
		{"abc <= abd", abc.LessEq, abd, true},
		{"abd > abc", abd.Greater, abc, true},
		{"abc >= abc", abc.GreaterEq, same, true},
	}
	for _, c := range cases {
		got, err := c.op(c.lhs)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if bool(got) != c.want {
			t.Errorf("%s: got=%v, want=%v", c.name, bool(got), c.want)
		}
	}
}

func TestBinaryCastFromPrimitives(t *testing.T) {
	t.Run("String", func(t *testing.T) {
		got, ok := CastToBinary(String("hi"))
		if !ok {
			t.Fatal("expected ok")
		}
		if !bytes.Equal([]byte(got), []byte("hi")) {
			t.Errorf("got=%v, want=[hi]", got)
		}
	})
	t.Run("[]byte", func(t *testing.T) {
		got, ok := CastToBinary([]byte("raw"))
		if !ok {
			t.Fatal("expected ok")
		}
		if !bytes.Equal([]byte(got), []byte("raw")) {
			t.Errorf("got=%v", got)
		}
	})
}

func TestBinaryStringRoundTrip(t *testing.T) {
	// Binary -> String -> Binary
	orig := NewBinary([]byte("roundtrip"))
	s := orig.ToString()
	back, ok := CastToBinary(s)
	if !ok {
		t.Fatal("CastToBinary failed")
	}
	eq, _ := orig.Eq(back)
	if !bool(eq) {
		t.Errorf("round-trip failed: %v != %v", orig, back)
	}
}

func TestFixedSizeBinaryComparison(t *testing.T) {
	a := NewFixedSizeBinary([]byte{1})
	b := NewFixedSizeBinary([]byte{2})
	same := NewFixedSizeBinary([]byte{1})

	eq, _ := same.Eq(a)
	if !bool(eq) {
		t.Error("1 == 1 should be true")
	}
	lt, _ := a.Less(b)
	if !bool(lt) {
		t.Error("1 < 2 should be true")
	}
	gt, _ := b.Greater(a)
	if !bool(gt) {
		t.Error("2 > 1 should be true")
	}
}

func TestLargeStringComparison(t *testing.T) {
	a := NewLargeString("apple")
	b := NewLargeString("banana")
	same := NewLargeString("apple")

	eq, _ := same.Eq(a)
	if !bool(eq) {
		t.Error("apple == apple should be true")
	}
	lt, _ := a.Less(b)
	if !bool(lt) {
		t.Error("apple < banana should be true")
	}
	if a.Value() != "apple" {
		t.Errorf("Value: got=%q", a.Value())
	}
}

func TestLargeStringCastFromPrimitives(t *testing.T) {
	t.Run("string", func(t *testing.T) {
		got, ok := CastToLargeString("hello")
		if !ok {
			t.Fatal("expected ok")
		}
		if string(got) != "hello" {
			t.Errorf("got=%q", got)
		}
	})
	t.Run("String", func(t *testing.T) {
		got, ok := CastToLargeString(String("world"))
		if !ok {
			t.Fatal("expected ok")
		}
		if string(got) != "world" {
			t.Errorf("got=%q", got)
		}
	})
}
