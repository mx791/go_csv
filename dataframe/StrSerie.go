package dataframe

import (
	"strings"
)

type StrSerie struct {
	values []string
}

func (s StrSerie) len() NumberSerie {
	values := make([]float64, len(s.values))
	for id, val := range s.values {
		values[id] = float64(len(val))
	}
	return NumberSerie{values}
}

func (s StrSerie) equals(value string) BoolSerie {
	values := boolSerieParallelise(func(id int) bool {
		return value == s.values[id]
	}, len(s.values))
	return BoolSerie{values}
}

func (s StrSerie) contains(value string) BoolSerie {
	values := boolSerieParallelise(func(id int) bool {
		return strings.Contains(s.values[id], value)
	}, len(s.values))
	return BoolSerie{values}
}
