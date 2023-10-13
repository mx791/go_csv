package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

const CSV_PATH = "D:\\dataset\\imdb\\movies.tsv"

func readCsv(path string) DataFrame {

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
			} else if letter == '\t' && !openBracket {
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

func main() {

	dataset := readCsv(CSV_PATH)
	fmt.Println(dataset.size())
	dataset = dataset.withColumn([]string{"startYear", "originalTitle", "runtimeMinutes", "tconst"})
	dataset = dataset.filter(
		dataset.series["startYear"].numberSerie().greaterThanScalar(2010.0))
	dataset.print(10)

	ratings := readCsv("D:\\dataset\\imdb\\ratings.tsv")
	ratings = ratings.filter(ratings.series["numVotes"].numberSerie().greaterThanScalar(800_000))
	ratings.print(100)

	merged := dataset.join(ratings, "tconst")
	merged = merged.iLoc(merged.series["numVotes"].numberSerie().argSort(false))
	merged.print(25)

}
