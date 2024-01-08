package main

import (
	"fmt"
	"testing"
	"github.com/mx791/go_csv/dataframe"
)

func TestCreatingNumericSeries(t *testing.T) {

	DF_SIZE := 150
	df := dataframe.DataFrameFromSeries(make(map[string]dataframe.Serie))
	df.SetColumn("index", dataframe.MakeRangeNumberSerie(0.0, 1.0, DF_SIZE).ToSerie())
	df.SetColumn("index_squared", df.Serie("index").NumberSerie().Mul(df.Serie("index").NumberSerie()).ToSerie())

	if df.Size() != DF_SIZE {
		t.Fatalf("Size error")
	}

	mean := df.Serie("index").NumberSerie().Mean()
	if mean < 0.49 || mean > 0.51 {
		t.Fatalf("Bad mean %f", mean)
	}

	mean = df.Serie("index_squared").NumberSerie().Mean()
	if mean < 0.32 || mean > 0.34 {
		t.Fatalf("Bad mean %f", mean)
	}
}

func TestPriceBlackScholes(t *testing.T) {
	DF_SIZE := 15000
	jumps := 1
	annualized_volatility := 0.25
	jump_vol := annualized_volatility / 19.1049
	strike := 100.0

	df := dataframe.DataFrameFromSeries(make(map[string]dataframe.Serie))
	base_returns := dataframe.MakeConstantNumberSerie(100.0, DF_SIZE)

	for i := 0; i < jumps; i++ {
		returns := dataframe.MakeNormalRandomNumberSerie(1.0, jump_vol, DF_SIZE)
		base_returns = base_returns.Mul(returns)
		df.SetColumn("day"+fmt.Sprint(i), base_returns.ToSerie())
	}

	df.SetColumn("pay_off", base_returns.AddScalar(-strike).ToSerie())

	df = df.Filter(df.Serie("pay_off").NumberSerie().GreaterThanScalar(0.0))
	option_price := df.Serie("pay_off").NumberSerie().Sum() / float64(DF_SIZE)

	expected_price := 0.522
	error := (option_price - expected_price) * (option_price - expected_price)

	if error > 0.01 {
		t.Fatalf("Wrong price %f vs %f", option_price, expected_price)
	}
}
