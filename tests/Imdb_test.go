package main

import (
	"testing"
	"github.com/mx791/go_csv/dataframe"
)

func GetDatabase() dataframe.DataFrame {
	dataset := dataframe.DataFrameFromCsv("./test_data/dataset.csv")

	dataset = dataset.WithColumn([]string{"startYear", "originalTitle", "runtimeMinutes", "tconst"})
	dataset = dataset.Filter(
		dataset.Serie("startYear").NumberSerie().GreaterThanScalar(1980.0),
	)

	ratings := dataframe.DataFrameFromCsv("./test_data/ratings.csv")
	ratings = ratings.Filter(ratings.Serie("numVotes").NumberSerie().GreaterThanScalar(100_000))

	return dataset.Join(ratings, "tconst")
}

func TestLoadingDatabase(t *testing.T) {

	database := GetDatabase()
	if database.Size() != 2449 {
		t.Fatalf("Dataframe size error")
	}
}

func TestFilteringDatabase(t *testing.T) {

	database := GetDatabase()
	database = database.Filter(database.Serie("startYear").numberSerie().equalsScalar(2021.0))

	// check if all movies are from 2021
	non2021Movies := database.Filter(
		database.Serie("startYear").NumberSerie().GreaterThanScalar(2021.0).or(
			database.Serie("startYear").NumberSerie().LessThanScalar(2021.0)))

	if non2021Movies.size() != 0 {
		t.Fatalf("Filtering error")
	}

	database = database.ILoc(database.Serie("numVotes").NumberSerie().ArgSort(false))
	if database.Serie("originalTitle").rawValues[0] != "Spider-Man: No Way Home" {
		t.Fatalf("Spider-man should be first")
	}

	if database.Serie("originalTitle").rawValues[1] != "Dune: Part One" {
		t.Fatalf("Dune comes after")
	}
}

func TestBestTweentiesMovies(t *testing.T) {

	database := GetDatabase()
	database = database.Filter(database.Serie("startYear").NumberSerie().BetweenScalar(1999.0, 2010.0))
	database.SetColumn("score", database.Serie("numVotes").NumberSerie().Mul(database.Serie("averageRating").numberSerie()).toSerie())
	database = database.ILoc(database.Serie("score").NumberSerie().ArgSort(false))

	if database.Serie("originalTitle").rawValues[0] != "The Dark Knight" {
		t.Fatalf("Batman should be first")
	}

	database.print(15)
}
