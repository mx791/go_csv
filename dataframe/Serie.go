package dataframe

import "sync"

/**
 * Classe de base pour les séries
 */
type Serie struct {
	rawValues []string
}

func (s Serie) boolSerie() BoolSerie {
	return makeBoolSerie(s)
}

func (s Serie) numberSerie() NumberSerie {
	return makeNumberSerie(s)
}

func (s Serie) strSerie() StrSerie {
	return StrSerie{s.rawValues}
}

/**
 * Série booléenne
 */
func makeBoolSerie(s Serie) BoolSerie {
	boolValues := make([]bool, len(s.rawValues))
	for id, val := range s.rawValues {
		if val == "1" || val == "true" || val == "True" {
			boolValues[id] = true
		} else {
			boolValues[id] = false
		}
	}
	return BoolSerie{boolValues}
}

type BoolSerie struct {
	values []bool
}

func (s BoolSerie) not() BoolSerie {
	boolValues := make([]bool, len(s.values))
	for id, val := range s.values {
		boolValues[id] = !val
	}
	return BoolSerie{boolValues}
}

func (s BoolSerie) or(s2 BoolSerie) BoolSerie {
	boolValues := make([]bool, len(s.values))
	for id, val := range s.values {
		boolValues[id] = val || s2.values[id]
	}
	return BoolSerie{boolValues}
}

func (s BoolSerie) and(s2 BoolSerie) BoolSerie {
	boolValues := make([]bool, len(s.values))
	for id, val := range s.values {
		boolValues[id] = val && s2.values[id]
	}
	return BoolSerie{boolValues}
}

func (s BoolSerie) conditional(condition BoolSerie, trueValue Serie, falseValue Serie) Serie {
	outValue := make([]string, len(s.values))
	for id, _ := range s.values {
		if condition.values[id] {
			outValue[id] = trueValue.rawValues[id]
		} else {
			outValue[id] = falseValue.rawValues[id]
		}
	}
	return Serie{outValue}
}

func boolSerieParallelise(fn func(index int) bool, serieSize int) []bool {
	values := make([]bool, serieSize)
	var wg sync.WaitGroup
	for i := 0; i < NUM_THREADS; i++ {
		wg.Add(1)
		go func() {
			for id := 0; id < serieSize; id += NUM_THREADS {
				values[id] = fn(id)
			}
			defer wg.Done()
		}()
		wg.Wait()
	}
	return values
}
