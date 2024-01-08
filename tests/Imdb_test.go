package main

import (
	"testing"
	"github.com/mx791/go_csv/dataframe"
)

func GetDatabase() DataFrame {
	dataset := dataframe.DataFrameFromCsv("./test_data/dataset.csv")

	dataset = dataset.withColumn([]string{"startYear", "originalTitle", "runtimeMinutes", "tconst"})
	dataset = dataset.filter(
		dataset.series["startYear"].numberSerie().greaterThanScalar(1980.0),
	)

	ratings := dataframe.DataFrameFromCsv("./test_data/ratings.csv")
	ratings = ratings.filter(ratings.series["numVotes"].numberSerie().greaterThanScalar(100_000))

	return dataset.join(ratings, "tconst")
}

func TestLoadingDatabase(t *testing.T) {

	database := GetDatabase()
	if database.size() != 2449 {
		t.Fatalf("Dataframe size error")
	}
}

func TestFilteringDatabase(t *testing.T) {

	database := GetDatabase()
	database = database.filter(database.series["startYear"].numberSerie().equalsScalar(2021.0))

	// check if all movies are from 2021
	non2021Movies := database.filter(
		database.series["startYear"].numberSerie().greaterThanScalar(2021.0).or(
			database.series["startYear"].numberSerie().lessThanScalar(2021.0)))

	if non2021Movies.size() != 0 {
		t.Fatalf("Filtering error")
	}

	database = database.iLoc(database.series["numVotes"].numberSerie().argSort(false))
	if database.series["originalTitle"].rawValues[0] != "Spider-Man: No Way Home" {
		t.Fatalf("Spider-man should be first")
	}

	if database.series["originalTitle"].rawValues[1] != "Dune: Part One" {
		t.Fatalf("Dune comes after")
	}
}

func TestBestTweentiesMovies(t *testing.T) {

	database := GetDatabase()
	database = database.filter(database.series["startYear"].numberSerie().betweenScalar(1999.0, 2010.0))
	database.setColumn("score", database.series["numVotes"].numberSerie().mul(database.series["averageRating"].numberSerie()).toSerie())
	database = database.iLoc(database.series["score"].numberSerie().argSort(false))

	if database.series["originalTitle"].rawValues[0] != "The Dark Knight" {
		t.Fatalf("Batman should be first")
	}

	database.print(15)
}
