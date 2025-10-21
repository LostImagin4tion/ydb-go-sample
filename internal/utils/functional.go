package utils

func Mapped[T any, R any](s *[]T, mapper func(int, T) R) []R {
	var mapped = make([]R, len(*s))
	for i, item := range *s {
		mapped[i] = mapper(i, item)
	}
	return mapped
}
