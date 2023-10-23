package main

import (
	"strings"
	"sync"
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
	values := make([]bool, len(s.values))
	var wg sync.WaitGroup
	for i := 0; i < NUM_THREADS; i++ {
		wg.Add(1)
		go func() {
			for id := 0; id < len(s.values); id += NUM_THREADS {
				values[id] = value == s.values[id]
			}
			defer wg.Done()
		}()
		wg.Wait()
	}

	return BoolSerie{values}
}

func (s StrSerie) contains(value string) BoolSerie {
	values := make([]bool, len(s.values))
	for id, val := range s.values {
		values[id] = strings.Contains(val, value)
	}
	return BoolSerie{values}
}
