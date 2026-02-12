package catbird

import "encoding/json"

type optionalSetter interface {
	setAbsent()
	setFromJSON([]byte) error
}

// Optional wraps a dependency output that may be absent.
type Optional[T any] struct {
	IsSet bool
	Value T
}

func (o *Optional[T]) setAbsent() {
	var zero T
	o.IsSet = false
	o.Value = zero
}

func (o *Optional[T]) setFromJSON(b []byte) error {
	var v T
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	o.IsSet = true
	o.Value = v
	return nil
}
