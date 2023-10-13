package main

import (
	"testing"
)

func TestCreatingNumericSeries(t *testing.T) {

	DF_SIZE := 150
	df := DataFrame{make(map[string]Serie)}
	df.setColumn("index", makeRangeNumberSerie(0.0, 1.0, DF_SIZE).toSerie())
	df.setColumn("index_squared", df.series["index"].numberSerie().mul(df.series["index"].numberSerie()).toSerie())

	if df.size() != DF_SIZE {
		t.Fatalf("Size error")
	}

	mean := df.series["index"].numberSerie().mean()
	if mean < 0.49 || mean > 0.51 {
		t.Fatalf("Bad mean %f", mean)
	}

	mean = df.series["index_squared"].numberSerie().mean()
	if mean < 0.32 || mean > 0.34 {
		t.Fatalf("Bad mean %f", mean)
	}
}
