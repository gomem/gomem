package gomem

import (
	"fmt"

	"github.com/gomem/gomem/pkg/collection"
	"github.com/gomem/gomem/pkg/logical"
	"github.com/gomem/gomem/pkg/object"
)

const (
	_ int = iota
	ObjectType
	CollectionType
	LogicalType
)

func NewGomemType(v interface{}) GomemType {
	switch vT := v.(type) {
	case logical.Logical:
		return GomemType{
			t: LogicalType,
			l: vT,
		}
	case collection.Collection:
		return GomemType{
			t: CollectionType,
			c: vT,
		}
	case object.Object:
		return GomemType{
			t: ObjectType,
			o: vT,
		}
	default:
		panic(fmt.Sprintf("%T is not a valid GomemType {Logical, Collection, Object}", v))
	}
}

type GomemType struct {
	t int
	o object.Object
	c collection.Collection
	l logical.Logical
}

func (t GomemType) Type() int {
	return t.t
}

func (t GomemType) Object() object.Object {
	return t.o
}

func (t GomemType) Collection() collection.Collection {
	return t.c
}

func (t GomemType) Logical() logical.Logical {
	return t.l
}
