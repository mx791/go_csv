package main

import (
	"testing"
)

func TestCreatingNumericSeries(t *testing.T) {

	DF_SIZE := 150
	df := DataFrame{make(map[string]Serie)}
	df.setColumn("index", makeRangeNumberSerie(0.0, 1.0, DF_SIZE))
	df.setColumn("index_squared", df.series["index"].numberSerie().mul(df.series["index"].numberSerie()))
	df.print(15)
}
