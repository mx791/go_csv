package dataframe

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
)

type NumberSerie struct {
	values []float64
}

func MakeNumberSerie(s Serie) NumberSerie {
	floatValues := make([]float64, len(s.rawValues))
	for id, val := range s.rawValues {
		newVal, err := strconv.ParseFloat(val, 64)
		if err == nil {
			floatValues[id] = newVal
		}
	}
	return NumberSerie{floatValues}
}

func MakeConstantNumberSerie(value float64, size int) NumberSerie {
	floatValues := make([]float64, size)
	for id, _ := range floatValues {
		floatValues[id] = value
	}
	return NumberSerie{floatValues}
}

func MakeRangeNumberSerie(start float64, end float64, size int) NumberSerie {
	floatValues := make([]float64, size)
	stepps := float64(size)
	for id, _ := range floatValues {
		floatValues[id] = start + (end-start)*float64(id)/stepps
	}
	return NumberSerie{floatValues}
}

func MakeUniformRandomNumberSerie(min float64, max float64, size int) NumberSerie {
	floatValues := make([]float64, size)
	for id, _ := range floatValues {
		floatValues[id] = rand.Float64()*(max-min) + min
	}
	return NumberSerie{floatValues}
}

func MakeNormalRandomNumberSerie(mean float64, std float64, size int) NumberSerie {
	floatValues := make([]float64, size)
	for id, _ := range floatValues {
		floatValues[id] = rand.NormFloat64()*std + mean
	}
	return NumberSerie{floatValues}
}

func (s NumberSerie) Add(s2 NumberSerie) NumberSerie {
	values := make([]float64, len(s.values))
	for id, val := range s.values {
		values[id] = val + s2.values[id]
	}
	return NumberSerie{values}
}

func (s NumberSerie) AddScalar(term float64) NumberSerie {
	values := make([]float64, len(s.values))
	for id, val := range s.values {
		values[id] = val + term
	}
	return NumberSerie{values}
}

func (s NumberSerie) Mean() float64 {
	return s.Sum() / float64(len(s.values))
}

func (s NumberSerie) Max() float64 {
	value := s.values[0]
	for _, val := range s.values {
		if val > value {
			value = val
		}
	}
	return value
}

func (s NumberSerie) Min() float64 {
	value := s.values[0]
	for _, val := range s.values {
		if val < value {
			value = val
		}
	}
	return value
}

func (s NumberSerie) Sum() float64 {
	value := 0.0
	for _, val := range s.values {
		value += val
	}
	return value
}

func (s NumberSerie) Sub(s2 NumberSerie) NumberSerie {
	values := make([]float64, len(s.values))
	for id, val := range s.values {
		values[id] = val - s2.values[id]
	}
	return NumberSerie{values}
}

func (s NumberSerie) Mul(s2 NumberSerie) NumberSerie {
	values := make([]float64, len(s.values))
	for id, val := range s.values {
		values[id] = val * s2.values[id]
	}
	return NumberSerie{values}
}
func (s NumberSerie) MulScalar(s2 float64) NumberSerie {
	values := make([]float64, len(s.values))
	for id, val := range s.values {
		values[id] = val * s2
	}
	return NumberSerie{values}
}

func (s NumberSerie) Div(s2 NumberSerie) NumberSerie {
	values := make([]float64, len(s.values))
	for id, val := range s.values {
		values[id] = val / s2.values[id]
	}
	return NumberSerie{values}
}

func (s NumberSerie) Equals(s2 NumberSerie) BoolSerie {
	values := make([]bool, len(s.values))
	for id, val := range s.values {
		values[id] = val == s2.values[id]
	}
	return BoolSerie{values}
}

func (s NumberSerie) EqualsScalar(s2 float64) BoolSerie {
	values := make([]bool, len(s.values))
	for id, val := range s.values {
		values[id] = val == s2
	}
	return BoolSerie{values}
}

func (s NumberSerie) GreaterThanScalar(s2 float64) BoolSerie {
	values := make([]bool, len(s.values))
	for id, val := range s.values {
		values[id] = val > s2
	}
	return BoolSerie{values}
}

func (s NumberSerie) LessThanScalar(s2 float64) BoolSerie {
	values := make([]bool, len(s.values))
	for id, val := range s.values {
		values[id] = val < s2
	}
	return BoolSerie{values}
}

func (s NumberSerie) BetweenScalar(min float64, max float64) BoolSerie {
	values := make([]bool, len(s.values))
	for id, val := range s.values {
		values[id] = val > min && val < max
	}
	return BoolSerie{values}
}

func (s NumberSerie) ToSerie() Serie {
	values := make([]string, len(s.values))
	for id, val := range s.values {
		values[id] = fmt.Sprintf("%f", val)
	}
	return Serie{values}
}

func (s NumberSerie) ArgSort(ascending bool) NumberSerie {
	valuesMap := make(map[float64][]float64, 0)
	for id, val := range s.values {
		vect, found := valuesMap[val]
		if found {
			vect = append(vect, float64(id))
			valuesMap[val] = vect
		} else {
			valuesMap[val] = []float64{float64(id)}
		}
	}

	sortedArray := make([]float64, len(s.values))
	copy(sortedArray, s.values)

	if !ascending {
		sort.Sort(sort.Reverse(sort.Float64Slice(sortedArray)))
	} else {
		sort.Float64s(sortedArray)
	}

	indexList := make([]float64, 0)

	for id := 0; id < len(sortedArray); id++ {
		indexList = append(indexList, valuesMap[sortedArray[id]]...)
		for id < len(sortedArray)-1 && sortedArray[id] == sortedArray[id+1] {
			id++
		}
	}

	return NumberSerie{indexList}
}
