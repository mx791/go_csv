package main

import (
	"fmt"
)

func main() {

	dataset := DataFrameFromCsv("D:\\dataset\\imdb\\movies.tsv")
	fmt.Println(dataset.size())
	dataset = dataset.withColumn([]string{"startYear", "originalTitle", "runtimeMinutes", "tconst"})
	dataset = dataset.filter(
		dataset.series["startYear"].numberSerie().greaterThanScalar(2000.0),
	)

	ratings := DataFrameFromCsv("D:\\dataset\\imdb\\ratings.tsv")
	ratings = ratings.filter(ratings.series["numVotes"].numberSerie().greaterThanScalar(800_000))

	merged := dataset.join(ratings, "tconst")

	starsColumn := merged.series["numVotes"].numberSerie().mul(merged.series["averageRating"].numberSerie().addScalar(-5.0))
	merged.setColumn("totalStars", starsColumn.toSerie())

	merged = merged.iLoc(merged.series["totalStars"].numberSerie().argSort(false))

	merged.print(25)

	merged.toCsv("./out.csv")

}
