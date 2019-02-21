
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val formatter = java.text.NumberFormat.getCurrencyInstance
val performatter = java.text.NumberFormat.getPercentInstance
val spark = SparkSession.builder.
  master("local[*]").
  config("spark.executor.memory", "8g").
  config("spark.driver.memory", "16g").
  config("spark.memory.offHeap.enabled",true).
  config("spark.memory.offHeap.size","16g").
  appName("Monthly Parquet ETL").
  getOrCreate()

import spark.implicits._
spark.sparkContext.setLogLevel("ERROR")

val training_src = spark.read.
  option("header", "true").
  option("inferSchema", "true").
  option("sep", ",").csv("/Users/cliftonjbest/Documents/Development/GWCM_Extracts/OS3RevisedTrainingDataSet.csv")

val training_src_05 = training_src.withColumn("part_num_prod_concat", concat($"Manufacture Part Number", $"PROD_DESC_BY_VENDOR"))

training_src_05.show()

val training_src_1 = training_src.filter($"PROD_DESC_BY_VENDOR" =!= "")
val training_src_2 = training_src_1.filter($"SUB_CATEGORY" =!= "")
val training_src_3 = training_src_2.filter($"Manufacture Part Number" =!= "")

import org.apache.spark.ml.feature.{RegexTokenizer}
val tokenizer = new RegexTokenizer().
  setInputCol("PROD_DESC_BY_VENDOR").
  setOutputCol("tokens")

val tokenized_src = tokenizer.transform(training_src_3)
tokenized_src.select($"SUB_CATEGORY",$"PROD_DESC_BY_VENDOR", $"tokens").show()

import org.apache.spark.ml.feature.HashingTF
val hashingTF = new HashingTF().
  setInputCol(tokenizer.getOutputCol).  // it does not wire transformers -- it's just a column name
  setOutputCol("features").
  setNumFeatures(5000)

val hashed_tokenized_src = hashingTF.transform(tokenized_src)

hashed_tokenized_src.select($"SUB_CATEGORY",$"PROD_DESC_BY_VENDOR", $"tokens", $"features").show()

import org.apache.spark.ml.feature.{IndexToString,StringIndexer}
val indexer = new StringIndexer().
  setHandleInvalid("keep").
  setInputCol("SUB_CATEGORY").
  setOutputCol("label")

val indexed_hased_tokenized_src = indexer.fit(hashed_tokenized_src).transform(hashed_tokenized_src).select($"SUB_CATEGORY",$"PROD_DESC_BY_VENDOR", $"tokens", $"features", $"label")
indexed_hased_tokenized_src.show()

val Array(trainDF, testDF) = indexed_hased_tokenized_src.randomSplit(Array(0.75, 0.25))

trainDF.select($"SUB_CATEGORY",$"PROD_DESC_BY_VENDOR", $"tokens", $"features", $"label").count()
testDF.select($"SUB_CATEGORY",$"PROD_DESC_BY_VENDOR", $"tokens", $"features", $"label").count()


import org.apache.spark.ml.classification.LogisticRegression
val lr = new LogisticRegression().setMaxIter(20).setRegParam(0.01)


import org.apache.spark.ml.Pipeline
val pipeline = new Pipeline().setStages(Array(lr))


import org.apache.spark.ml.classification.{LinearSVC, OneVsRest}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

//val lsvc = new LinearSVC().
//  setMaxIter(10).
//  setRegParam(0.1)

// Fit the model
//val lsvcModel = lsvc.fit(training)


// instantiate the base classifier
val classifier = new LinearSVC().
  setMaxIter(10).
  setRegParam(0.1)

// instantiate the One Vs Rest Classifier.
val ovr = new OneVsRest().
  setClassifier(classifier)

// train the multiclass model.
val ovrModel = ovr.fit(trainDF)

// score the model on test data.
val predictions = ovrModel.transform(testDF)

val evaluator = new MulticlassClassificationEvaluator().
  setMetricName("accuracy")

// compute the classification error on test data.
val accuracy = evaluator.evaluate(predictions)
println(s"Test Error = ${1 - accuracy}")


val model = pipeline.fit(trainDF)

val trainPredictions = model.transform(trainDF)

val testPredictions = model.transform(testDF)
val evaluator = new MulticlassClassificationEvaluator().
  setMetricName("accuracy")

// compute the classification error on test data.
val accuracy = evaluator.evaluate(testPredictions)
println(s"Test Error = ${1 - accuracy}")


trainPredictions.select("PROD_DESC_BY_VENDOR", "SUB_CATEGORY", "prediction", "label").show

