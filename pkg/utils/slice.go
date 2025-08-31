package utils

func MapSlice[T any, R any](slice []T, mapFunc func(T) R) []R {
	result := make([]R, len(slice))
	for idx, el := range slice {
		result[idx] = mapFunc(el)
	}
	return result
}

func SliceRemoveAt[T any](slice []T, idx int) (result T, contains bool) {
	if idx < 0 || idx >= len(slice) {
		contains = false
		return
	}

	result = slice[idx]
	contains = true
	slice = append(slice[:idx], slice[idx+1:]...)
	return
}
