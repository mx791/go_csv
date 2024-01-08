package main

import (
	"testing"
	"github.com/mx791/go_csv/dataframe"
)

func createDataframe() dataframe.DataFrame {
	df := dataframe.DataFrame{make(map[string]dataframe.Serie)}
	df.SetColumn("names", dataframe.Serie{[]string{"A", "B", "C", "D"}})
	df.SetColumn("ages", dataframe.Serie{[]string{"12", "15", "12", "7"}})
	return df
}

func TestCreateCsvAndReadit(t *testing.T) {

	df := createDataframe()

	if df.Size() != 4 {
		t.Fatalf("Size error")
	}

	df.ToCsv("./out.csv")

	df2 := dataframe.DataFrameFromCsv("./out.csv")

	if df2.Size() != 4 {
		t.Fatalf("Size error")
	}

}
