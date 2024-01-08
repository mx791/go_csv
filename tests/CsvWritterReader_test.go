package main

import (
	"testing"
	"github.com/mx791/go_csv/dataframe"
)

func createDataframe() dataframe.DataFrame {
	df := dataframe.DataFrame{make(map[string]Serie)}
	df.setColumn("names", Serie{[]string{"A", "B", "C", "D"}})
	df.setColumn("ages", Serie{[]string{"12", "15", "12", "7"}})
	return df
}

func TestCreateCsvAndReadit(t *testing.T) {

	df := createDataframe()

	if df.size() != 4 {
		t.Fatalf("Size error")
	}

	df.toCsv("./out.csv")

	df2 := dataframe.DataFrameFromCsv("./out.csv")

	if df2.size() != 4 {
		t.Fatalf("Size error")
	}

}
