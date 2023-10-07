package types

// To-Do, Investigate Arrow Math & Gonum Etc
type StatsCalculator interface {
	Sum() float64
	Min() float64
	Mode() float64
	Max() float64
	StdDev() float64
	Mean() float64
	Median() float64
	Abs() float64
	Variance() float64
	Product() float64
	Quantile(f float32) float64
}
