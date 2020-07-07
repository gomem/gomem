package logical

import (
	"github.com/apache/arrow/go/arrow/array"
	"github.com/gomem/gomem/pkg/collection"
	"github.com/gomem/gomem/pkg/object"
)

// NewLogicalList creates a new logical List builder.
func NewLogicalList(builder *array.ListBuilder) *LogicalList {
	return &LogicalList{
		builder: builder,
	}
}

// LogicalList is a logical collection type.
// Because LogicalList is a logical type,
// it can hold both Objects and Collections.
type LogicalList struct {
	builder *array.ListBuilder
}

// AppendObject can append an Object.
func (c *LogicalList) AppendObject(v object.Object) error {
	if v == nil {
		c.builder.AppendNull()
		return nil
	}

	// TODO(nickpoorman): Implement
	// b, chkd := object.CastToList(v)
	// if !chkd {
	// 	return fmt.Errorf("cannot cast %T to object.List", v)
	// }

	// c.builder.Append(b.Value())
	return nil
}

// AppendCollection can append a Collection.
func (c *LogicalList) AppendCollection(v collection.Collection) error {
	if v == nil {
		c.builder.AppendNull()
		return nil
	}

	// TODO(nickpoorman): Implement
	// b, chkd := object.CastToList(v)
	// if !chkd {
	// 	return fmt.Errorf("cannot cast %T to object.List", v)
	// }

	// c.builder.Append(b.Value())
	return nil
}
