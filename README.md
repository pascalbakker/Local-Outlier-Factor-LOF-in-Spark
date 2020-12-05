# Local-Outlier-Detection-Spark
Big Data Management Final Project

**Goal:** Implement LOF in a distributed manner utilizing Spark

**Authors:** Pascal Bakker and Mario Arduz

### Install and Compile:

	sbt compile

### Run

	sbt run

Results will be located in data/results/lof.txt

Data is read from data/data.csv

## Libraries
	1. Spark 3.0.1
	2. Spark SQL 3.0.1
	3. Spark MLLib 3.0.1
	4. Breeze 1.1
	5. Breeze Natives 1.1
	6. Breeze Viz 1.1

## Finished
	1. LOF
	2. Generates dataset containing 2D random data
	3. Plot of original data

## TODO
	1. Test to ensure LOF is working correctly. Compare results with other LOF implementation results(sklearn)
	2. Test with large dataset (>=100MB) and benchmark results
	3. Create plot with data.csv color coded with results. (if data has LOF >= 1 then color red else color green)
	4. Add different types of distance calculations such as Jacardian(Low Priority)


### Documentation

Get LOF above 1
	awk -F, 'gsub(/[()]/,"") && $2 >= 1.0 {print $1, $2}' data/results/lof.txt/part-00000

Get Number of Points who are anomolies
	awk -F, 'gsub(/[()]/,"") && $2 >= 1.0 {print $1, $2}' data/results/lof.txt/part-00000 | wc -l
