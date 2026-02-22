package catbird

// Optional wraps a dependency output that may be absent.
type Optional[T any] struct {
	IsSet bool
	Value T
}
