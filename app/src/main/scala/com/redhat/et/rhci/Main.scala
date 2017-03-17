package com.redhat.et.rhci

import org.apache.spark.sql.SparkSession
import scopt.OptionParser

case class PConfig(data: String="data.parquet", output: String="out.parquet", features: Int=512, partitions: Int=256, savemode: String="overwrite", master: Option[String]=None)

object Main {
  def main(args: Array[String]) {
    args.toList match {
      case hd::tl =>
	hd match {
	  case "preprocess" =>
	    Preprocessor.main(tl.toArray)
	  case "afforest" =>
	    Afforest.main(tl.toArray)
	  case _ =>
	    Console.println("valid commands include \"preprocess\", \"afforest\"")
	}
      case Nil =>
	Console.println("specify a command:  valid commands include \"preprocess\", \"afforest\"")
    }
  }
}


object Preprocessor {
  def main(args: Array[String]) {
    val parser = new OptionParser[PConfig]("") {
      head("rhci preprocess", "0.0.1")

      opt[String]("master")
	.action((x,c) => c.copy(master=Some(x)))
	.text("spark master to use (defaults to value of spark.master property (if set) or \"local[*]\" (if not))")

      opt[String]("data")
	.action((x,c) => c.copy(data=x))
	.text("path of input data (defaults to \"data.parquet\")")

      opt[String]('o', "output")
	.action((x,c) => c.copy(output=x))
	.text("destination for parquet output (defaults to \"out.parquet\")")

      opt[String]("savemode")
	.action((x,c) => c.copy(savemode=x))
	.text("what to do if the output file already exists (default is \"overwrite\")")

      opt[Int]("features")
	.action((x,c) => c.copy(features=x))
	.text("number of features to generate for each message")

      opt[Int]("partitions")
	.action((x,c) => c.copy(partitions=x))
	.text("number of partitions to generate in output file")
    }
    
    parser.parse(args, PConfig()) match {
      case Some(config) =>
	val sesh = SparkSession.builder().master(config.master.getOrElse(System.getProperty("spark.master", "local[*]"))).getOrCreate()
	val data = config.data
	val output = config.output
	val savemode = config.savemode
	val features = config.features
	val partitions = config.partitions

	val sqlc = sesh.sqlContext
	val df = sesh.read.parquet(data)

	import org.apache.spark.sql.functions._
	import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer, StopWordsRemover}

	val pro = df
		   .filter(df("message").isNotNull)
		   .select(df("ci_job.build_id"), when(df("ci_job.status") === "SUCCESS", 0).otherwise(1).alias("status"), lower(df("message")).alias("message"))
	
	val tokenizer = new Tokenizer().setInputCol("message").setOutputCol("rawTokens")
	val remover = new StopWordsRemover().setInputCol("rawTokens").setOutputCol("tokens")
	val htf = new HashingTF().setNumFeatures(128).setInputCol("tokens").setOutputCol("features")
	val hashed = htf.transform(remover.transform(tokenizer.transform(pro)))

	hashed.select("build_id", "status", "message", "features").write.mode(savemode).save(output)

	sesh.sparkContext.stop
      case None => ()
    }
  }
}




case class AConfig(data: String="out.parquet", output: String="forest.model", savemode: String="overwrite", trainSample: Double=0.7, master: Option[String]=None)

object Afforest {
  def main(args: Array[String]) {
    val parser = new OptionParser[AConfig]("") {
      head("rhci afforest", "0.0.1")

      opt[String]("master")
	.action((x,c) => c.copy(master=Some(x)))
	.text("spark master to use (defaults to value of spark.master property (if set) or \"local[*]\" (if not))")

      opt[String]("data")
	.action((x,c) => c.copy(data=x))
	.text("path of preprocessed input data (defaults to \"out.parquet\")")

      opt[String]('o', "output")
	.action((x,c) => c.copy(output=x))
	.text("destination for parquet output (defaults to \"forest.model\")")

      opt[String]("savemode")
	.action((x,c) => c.copy(savemode=x))
	.text("what to do if the output file already exists (default is \"overwrite\")")

      opt[Double]("sample")
	.action((x,c) => c.copy(trainSample=x))
	.text("fraction of rows to use as training sample")
    }
    
    parser.parse(args, AConfig()) match {
      case Some(config) =>
	val sesh = SparkSession.builder().master(config.master.getOrElse(System.getProperty("spark.master", "local[*]"))).getOrCreate()
	val data = config.data
	val output = config.output
	val savemode = config.savemode
	val trainSample = config.trainSample

	val sqlc = sesh.sqlContext
	val df = sesh.read.parquet(data)

	import org.apache.spark.sql.functions._
	import org.apache.spark.ml.classification.RandomForestClassifier

	val rf = new RandomForestClassifier().setLabelCol("status").setFeaturesCol("features")
	val Array(training, test) = df.randomSplit(Array(trainSample, 1.0d - trainSample))
	val model = rf.fit(training)
	
	sesh.sparkContext.stop
      case None => ()
    }
  }
}
