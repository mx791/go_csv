package dataframe

import (
	"strings"
)

type StrSerie struct {
	values []string
}

func (s StrSerie) Len() NumberSerie {
	values := make([]float64, len(s.values))
	for id, val := range s.values {
		values[id] = float64(len(val))
	}
	return NumberSerie{values}
}

func (s StrSerie) Equals(value string) BoolSerie {
	values := BoolSerieParallelise(func(id int) bool {
		return value == s.values[id]
	}, len(s.values))
	return BoolSerie{values}
}

func (s StrSerie) Contains(value string) BoolSerie {
	values := BoolSerieParallelise(func(id int) bool {
		return strings.Contains(s.values[id], value)
	}, len(s.values))
	return BoolSerie{values}
}
