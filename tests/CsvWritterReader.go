package main

import (
	"fmt"
	"testing"
)

func TestCreateCsvAndReadit(t *testing.T) {

	df := DataFrame{make(map[string]Serie)}
	df.setColumn("names", Serie{[]string{"A", "B", "C", "D"}})
	df.setColumn("ages", Serie{[]string{"12", "15", "12", "7"}})
	fmt.Println(df.size())

}
