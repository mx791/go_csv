package main

import (
	"fmt"
	"testing"
)

func TestCreateCsvAndReadit(t *testing.T) {

	df := DataFrame{make(map[string]Serie)}
	df.setColumn("names", Serie{[]string{"A", "B", "C", "D"}})
	df.setColumn("ages", Serie{[]string{"12", "15", "12", "7"}})
	df.print(10)

	if df.size() != 4 {
		t.Fatalf("Size error")
	}

	df.toCsv("./out.csv")

	df2 := DataFrameFromCsv("./out.csv")
	df2.print(10)
	if df2.size() != 4 {
		t.Fatalf("Size error")
	}

}
