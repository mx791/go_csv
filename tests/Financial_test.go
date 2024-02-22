package main

import (
	  "testing"
	  "github.com/mx791/go_csv/dataframe"
	  "fmt"
)

func TestSnP500(t *testing.T) {
	dataframe.CSV_READER_SEPARTOR = ','
    	dataset := dataframe.DataFrameFromCsv("./test_data/sp500.csv")
	
	dataset.Print(15)
	fmt.Println(dataset.Size())
	
	if dataset.Size() != 1769 {
		t.Fatalf("Size error")
	}
}
