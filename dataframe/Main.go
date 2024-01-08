package dataframe

import (
	"fmt"
)

func main() {

	dataset := DataFrameFromCsv("D:\\dataset\\imdb\\movies.tsv")
	fmt.Println(dataset.Size())
	dataset = dataset.WithColumn([]string{"startYear", "originalTitle", "runtimeMinutes", "tconst"})
	dataset = dataset.Filter(
		dataset.series["startYear"].NumberSerie().greaterThanScalar(2000.0),
	)

	ratings := DataFrameFromCsv("D:\\dataset\\imdb\\ratings.tsv")
	ratings = ratings.Filter(ratings.series["numVotes"].NumberSerie().greaterThanScalar(800_000))

	merged := dataset.Join(ratings, "tconst")

	starsColumn := merged.series["numVotes"].NumberSerie().mul(merged.series["averageRating"].NumberSerie().addScalar(-5.0))
	merged.SetColumn("totalStars", starsColumn.toSerie())

	merged = merged.ILoc(merged.series["totalStars"].NumberSerie().ArgSort(false))

	merged.Print(25)

	merged.ToCsv("./out.csv")

}
