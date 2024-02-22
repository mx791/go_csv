package main

import (
	  "testing"
	  "github.com/mx791/go_csv/dataframe"
)

func TestSnP500(t *testing.T) {

    dataset := dataframe.DataFrameFromCsv("./test_data/sp500.csv")
}
