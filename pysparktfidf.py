from pyspark.ml.feature import Tokenizer, HashingTF, IDF
from pyspark.sql import SparkSession

# Set up Spark session
spark = SparkSession.builder.appName("TFIDF").getOrCreate()

# Load CSV data
data = spark.read.csv("s3://inputdata5/hamcsvouput.csv", header=True, inferSchema=True)

# Tokenize the text column
tokenizer = Tokenizer(inputCol="text_column", outputCol="words")
words_data = tokenizer.transform(data)

# Calculate Term Frequencies (TF)
hashingTF = HashingTF(inputCol="words", outputCol="raw_features", numFeatures=10000)
featurized_data = hashingTF.transform(words_data)

# Calculate Inverse Document Frequencies (IDF)
idf = IDF(inputCol="raw_features", outputCol="features")
idf_model = idf.fit(featurized_data)
tfidf_data = idf_model.transform(featurized_data)

# Display the TF-IDF results
tfidf_data.select("text_column", "features").show(truncate=False)

# Stop Spark session
spark.stop()
