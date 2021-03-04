# import necessary libraries
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pandas as pd

# manualTest = True
manualTest = False

# get the parsed data csv file
pathToCsv = '/tmp/files/parsedData.csv'
# pathToCsv = '/tmp/files/outTest.csv'

# the file in which the final data is saved
saveCsv = '/tmp/files/topTopics.csv'
# saveCsv = '/tmp/files/testTopics.csv'

# create sparksession
spark = SparkSession \
    .builder \
    .appName("Pysparkexample") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# gat the data and pass it ot a spark dataframe
df = spark.read.csv(pathToCsv, header="true")

# get top 5 topics overall
dfTopTopics = df.groupBy('topics').agg(F.count('topics'))\
    .orderBy('count(topics)', ascending=False).limit(5)

topTopics = {}

# save the top 5 topics
topTopics['global_top_5'] = [row['topics'] for row in dfTopTopics .collect()]

# get the top 5 cities by number of topics
dfTopCities = df.groupBy('city').agg(F.count('topics'))\
    .orderBy('count(topics)', ascending=False).limit(5)

# get a list woth only the city names
topCities = [row['city'] for row in dfTopCities.collect()]

# get the top 5 topics for each of the top cities
for item in topCities:
    dfcity = df.where(df.city == item)\
        .groupBy('topics').agg(F.count('topics'))\
        .orderBy('count(topics)', ascending=False).limit(5)
    # save the top topics for each city
    topTopics[item] = [row['topics'] for row in dfcity.collect()]

# transform the data into a Pandas dataframe
pdDf = pd.DataFrame(topTopics)

# save the data to the csv
pdDf.to_csv(saveCsv, encoding='utf8', index=False)
