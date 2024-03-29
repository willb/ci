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
	  case "organize" =>
	    Organize.main(tl.toArray)
	  case _ =>
	    Console.println("valid commands include \"preprocess\", \"afforest\", \"organize\"")
	}
      case Nil =>
	Console.println("specify a command:  valid commands include \"preprocess\", \"afforest\", \"organize\"")
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

      help("help").text("prints this help text")
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
	.text("destination for serialized model output (defaults to \"forest.model\")")

      opt[String]("savemode")
	.action((x,c) => c.copy(savemode=x))
	.text("what to do if the output file already exists (default is \"overwrite\")")

      opt[Double]("sample")
	.action((x,c) => c.copy(trainSample=x))
	.text("fraction of rows to use as training sample")

      help("help").text("prints this help text")
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
	
	model.save(output)
      
	sesh.sparkContext.stop
      case None => ()
    }
  }
}

case class OConfig(data: String="out.parquet", output: String="som", savemode: String="overwrite", master: Option[String]=None, xdim: Int=10, ydim: Int=10)

object Organize {
  def main(args: Array[String]) {
    val parser = new OptionParser[OConfig]("") {
      head("rhci organize", "0.0.1")

      opt[String]("master")
	.action((x,c) => c.copy(master=Some(x)))
	.text("spark master to use (defaults to value of spark.master property (if set) or \"local[*]\" (if not))")

      opt[String]("data")
	.action((x,c) => c.copy(data=x))
	.text("path of preprocessed input data (defaults to \"out.parquet\")")

      opt[String]('o', "output")
	.action((x,c) => c.copy(output=x))
	.text("prefix for model output (defaults to \"output\")")

      opt[Int]('x', "xdim")
	.action((x,c) => c.copy(xdim=x))
	.text("width of map (defaults to 10)")

      opt[Int]('y', "ydim")
	.action((x,c) => c.copy(ydim=x))
	.text("height of map (defaults to 10)")

      help("help").text("prints this help text")
    }
    
    parser.parse(args, OConfig()) match {
      case Some(config) =>
	val sesh = SparkSession.builder().master(config.master.getOrElse(System.getProperty("spark.master", "local[*]"))).getOrCreate()
	val data = config.data
	val output = config.output
	val xdim = config.xdim
	val ydim = config.ydim
	
	val sqlc = sesh.sqlContext
	val df = sesh.read.parquet(data)

	import org.apache.spark.sql.Row
	import org.apache.spark.sql.functions._
	import org.apache.spark.mllib.linalg.{Vector => SV}

	import com.redhat.et.silex.som.SOM

	val successes = df.filter(df("status") === 0).select("features").rdd.map { case Row(s: SV) => s }
	val failures = df.filter(df("status") === 1).select("features").rdd.map { case Row(s: SV) => s }
	val fdim = (successes.take(1))(0).size
	val successmap = SOM.train(xdim, ydim, fdim, 20, successes, sigmaScale=0.7)
	val failuremap = SOM.train(xdim, ydim, fdim, 20, failures, sigmaScale=0.7)

	SOM.save(successmap, s"$output-successes.som")
	SOM.save(failuremap, s"$output-failures.som")
      
	sesh.sparkContext.stop
      case None => ()
    }
  }  
}
