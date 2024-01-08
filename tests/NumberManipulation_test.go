package main

import (
	"fmt"
	"testing"
	"github.com/mx791/go_csv/dataframe"
)

func TestCreatingNumericSeries(t *testing.T) {

	DF_SIZE := 150
	df := src.DataFrame{make(map[string]Serie)}
	df.setColumn("index", makeRangeNumberSerie(0.0, 1.0, DF_SIZE).toSerie())
	df.setColumn("index_squared", df.series["index"].numberSerie().mul(df.series["index"].numberSerie()).toSerie())

	if df.size() != DF_SIZE {
		t.Fatalf("Size error")
	}

	mean := df.series["index"].numberSerie().mean()
	if mean < 0.49 || mean > 0.51 {
		t.Fatalf("Bad mean %f", mean)
	}

	mean = df.series["index_squared"].numberSerie().mean()
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

	df := src.DataFrame{make(map[string]Serie)}
	base_returns := src.makeConstantNumberSerie(100.0, DF_SIZE)

	for i := 0; i < jumps; i++ {
		returns := src.makeNormalRandomNumberSerie(1.0, jump_vol, DF_SIZE)
		base_returns = base_returns.mul(returns)
		df.setColumn("day"+fmt.Sprint(i), base_returns.toSerie())
	}

	df.setColumn("pay_off", base_returns.addScalar(-strike).toSerie())

	df = df.filter(df.series["pay_off"].numberSerie().greaterThanScalar(0.0))
	option_price := df.series["pay_off"].numberSerie().sum() / float64(DF_SIZE)

	expected_price := 0.522
	error := (option_price - expected_price) * (option_price - expected_price)

	if error > 0.01 {
		t.Fatalf("Wrong price %f vs %f", option_price, expected_price)
	}
}
