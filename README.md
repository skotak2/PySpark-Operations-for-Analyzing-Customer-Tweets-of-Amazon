# PySpark Operations on Customer Reviews of Amazon
This exercise will showcase how to perform operations using Spark and find the most repeated words on the busiest days

### TABLE OF CONTENTS
* [Objective](#objective)
* [Technologies](#technologies)
* [Algorithms](#algorithms)
* [Data](#data)
* [Implementation](#implementation)
* [Results](#results)

## OBJECTIVE 
* Showcase implementation of Spark context and Spark SQL context on Amazon Tweets data with about 400k tweets
* Find the days with high influx of tweets dealing with - Tweet_id(id_str), Tweet_created_time, Retweet count and Favourite_count
* Analyze the subset of tweets on the busiest day and find the most repeated words 

## TECHNOLOGIES
Project is created with:

* PySpark

## ALGORITHMS

* MapReduce in Spark

## Data

The used dataset is a collection of tweets where Amazon was tagged by the users on Twitter to review its service. For each and every tweet/record in dataset, the sentiment of the tweet is also recorded and that is the binary target variable for the dataset. As the dataset consists of 400k records, we cannot build and run machine learning models directly and we would need to have a Big Data service to distribute and run the program. So, PySpark was used as it is easy to integrate it with Python for analysis.

## IMPLEMENTATION

**1. Import csv file:**
```python
data = spark.read.format("csv").option("header","true").load("filepath/filename.csv")
```
**2. Selecting the required columns from a data frame:**
```python
data.select("column_name1",'colname2','colname3','colname4')
```
**3. Printing data types of columns:**
```python
types = [f.dataType for f in data1.schema.fields]
```

**4. Printing distinct values of a column:**
```python
data.select("column_name").distinct().show()
```
**5. Parsing a column to create additional columns:**
```python
from pyspark.sql.functions import split
tweet_created_at is in the format: "Tue Nov 01 02:39:55 +0000 2016"
a = split(dat_filtered["tweet_created_at"], ' ')
dat_filtered = dat_filtered.withColumn('Month', a.getItem(1))
dat_filtered = dat_filtered1.withColumn('Date', a.getItem(2))
dat_filtered = dat_filtered1.withColumn('Year', a.getItem(5))
```
**6. Concatenating multiple columns to form a new one**
```python
dat_filtered.select(concat(col("Month"), lit(" "), col("Date"),lit(" "), col("Year")).alias("Date"))
```
**7. Importing SQL funtions col,lit**
```python
import pyspark.sql.functions as sq dat_filtered.withColumn("tweet_created_at",sq.concat(col("Month"), sq.lit(" "), sq.col("Date"),sq.lit(" "), sq.col("Year")))
```
**8. Aggregations on dataframe**
Command counts the number of tweets grouped by the date
```python
df.groupby(df.date).agg(sq.count('id_str').alias("count_of_tweets"))
```


**9. Initializing spark context and sql context to perform SQL queries**
```python
conf = pyspark.SparkConf()
sc = pyspark.SparkContext.getOrCreate(conf=conf)
from pyspark.sql import SQLContext
sqlcontext = SQLContext(sc)
counts.registerTempTable("tmpcounts")
counts_ordered = sqlcontext.sql("SELECT * FROM tmpcounts order by count_of_tweets desc limit 5")
```
## RESULTS

The above illustrated operations on 400k records was done in 3 minutes of times against 15+ minutes of time with the python environment.



