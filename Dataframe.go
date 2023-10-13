package main

import (
	"fmt"
)

type DataFrame struct {
	series map[string]Serie
}

func (d DataFrame) size() int {
	for _, val := range d.series {
		return len(val.rawValues)
	}
	return 0
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func valueAtSize(data string, targetSize int) string {
	out := data[0:min(targetSize, len(data))]
	for len(out) < targetSize {
		out = out + " "
	}
	return out
}

func (d DataFrame) print(size int) {

	columnsNames := make([]string, 0)
	for col, _ := range d.series {
		columnsNames = append(columnsNames, col)
	}

	for range d.series {
		fmt.Print("----------------")
	}
	fmt.Print("\n")

	fmt.Print("|")
	for _, col := range columnsNames {
		fmt.Print(valueAtSize(col, 15) + "|")
	}
	fmt.Print("\n")
	for range d.series {
		fmt.Print("----------------")
	}
	fmt.Print("\n")
	for i := 0; i < size; i++ {
		fmt.Print("|")
		for _, col := range columnsNames {
			fmt.Print(valueAtSize(d.series[col].rawValues[i], 15) + "|")
		}
		fmt.Print("\n")
	}

	if d.size() >= size*2 {

		fmt.Println("| ...", d.size()-size*2, " lines...")

		for i := d.size() - size; i < d.size(); i++ {
			fmt.Print("|")
			for _, col := range columnsNames {
				fmt.Print(valueAtSize(d.series[col].rawValues[i], 15) + "|")
			}
			fmt.Print("\n")
		}
	}

	//fmt.Print("\n")
	for range d.series {
		fmt.Print("----------------")
	}
	fmt.Print("\n")
}

func (d DataFrame) filter(filter BoolSerie) DataFrame {
	columnsNames := make([]string, 0)
	df := make(map[string]Serie, 0)
	for col, _ := range d.series {
		columnsNames = append(columnsNames, col)
		df[col] = Serie{make([]string, 0)}
	}

	for id, val := range filter.values {
		if val {
			for _, col := range columnsNames {
				s := df[col]
				s.rawValues = append(s.rawValues, d.series[col].rawValues[id])
				df[col] = s
			}
		}
	}
	return DataFrame{df}
}

func (d DataFrame) iLoc(indexList NumberSerie) DataFrame {
	columnsNames := make([]string, 0)
	df := make(map[string]Serie, 0)
	for col, _ := range d.series {
		columnsNames = append(columnsNames, col)
		df[col] = Serie{make([]string, 0)}
	}

	for _, val := range indexList.values {
		val2 := int(val)
		if val2 >= d.size() {
			continue
		}
		for _, col := range columnsNames {
			s := df[col]
			s.rawValues = append(s.rawValues, d.series[col].rawValues[val2])
			df[col] = s
		}
	}
	return DataFrame{df}
}

func (d DataFrame) join(d2 DataFrame, colName string) DataFrame {

	rightValues := make(map[string]int)
	for id, val := range d2.series[colName].rawValues {
		rightValues[val] = id
	}

	rightIdList := make([]float64, 0)
	leftIdList := make([]float64, 0)
	for id, val := range d.series[colName].rawValues {
		indx, found := rightValues[val]
		if found {
			rightIdList = append(rightIdList, float64(indx))
			leftIdList = append(leftIdList, float64(id))
		}
	}

	rightDataframe := d2.iLoc(NumberSerie{rightIdList})
	leftDataframe := d.iLoc(NumberSerie{leftIdList})

	newDataframe := make(map[string]Serie, 0)
	for col, serie := range rightDataframe.series {
		newDataframe[col] = serie
	}
	for col, serie := range leftDataframe.series {
		newDataframe[col] = serie
	}

	return DataFrame{newDataframe}
}

func (d DataFrame) withColumn(columns []string) DataFrame {
	newDf := make(map[string]Serie)
	for _, col := range columns {
		newDf[col] = d.series[col]
	}
	return DataFrame{newDf}
}
