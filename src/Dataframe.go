package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
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

var CSV_READER_SEPARTOR byte = '\t'
var NUM_THREADS = 1

func DataFrameFromCsv(path string) DataFrame {

	separator := '\t'
	file, err := os.Open(path)
	if err != nil {
		return DataFrame{}
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Scan()
	firstLine := scanner.Text()

	size := 0

	columnsNames := strings.Split(firstLine, string(separator))
	series := make([][]string, len(columnsNames))

	for scanner.Scan() {

		currentLine := scanner.Text()
		var sb strings.Builder
		valueArray := make([]string, 0)
		openBracket := false

		for id := 0; id < len(currentLine); id++ {
			letter := currentLine[id]
			if letter == '"' {
				openBracket = !openBracket
				continue
			} else if letter == CSV_READER_SEPARTOR && !openBracket {
				valueArray = append(valueArray, sb.String())
				sb = strings.Builder{}
			} else {
				sb.Write([]byte{letter})
			}
		}
		valueArray = append(valueArray, sb.String())

		if len(valueArray) == len(columnsNames) {
			size += 1

			for id := 0; id < len(columnsNames); id++ {
				series[id] = append(series[id], valueArray[id])
			}
		}
	}

	seriesMap := make(map[string]Serie, 0)
	for id, col := range columnsNames {
		seriesMap[col] = Serie{series[id]}
	}

	return DataFrame{seriesMap}
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
	for i := 0; i < min(size, d.size()); i++ {
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

func (d DataFrame) setColumn(name string, column Serie) {
	d.series[name] = column
}

func (d DataFrame) toCsv(filePath string) {
	var sb strings.Builder

	cc := 0
	columnsNames := make([]string, 0)
	for col, _ := range d.series {
		columnsNames = append(columnsNames, col)
		sb.WriteString(col)
		cc++
		if cc < len(d.series) {
			sb.WriteString(string(CSV_READER_SEPARTOR))
		}
	}
	sb.WriteString("\n")

	for id := 0; id < d.size(); id++ {
		for cc, col := range columnsNames {
			sb.WriteString(d.series[col].rawValues[id])
			if cc < len(columnsNames)-1 {
				sb.WriteString(string(CSV_READER_SEPARTOR))
			}
		}
		sb.WriteString("\n")
	}

	f, _ := os.Create(filePath)
	defer f.Close()
	w := bufio.NewWriter(f)
	w.WriteString(sb.String())
	w.Flush()

}
