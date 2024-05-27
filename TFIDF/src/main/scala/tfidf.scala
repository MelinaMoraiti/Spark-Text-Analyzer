import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.sql.functions._

object tfidf {

  def main(args: Array[String]): Unit = {
  

    // Check if the input path is provided
    if (args.length < 1) {
      println("Usage: ./run-spark.sh <File_with_main_class> <number_of_threads> <name_of_jar_file> <fileset path>")
      System.exit(1)
    }
    // Use the input path from command line arguments
    val inputPath = args(0)
    // Use the output path from command line arguments
    //val outputPath = args(1)
    // Create spark configuration
    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("TFIDF")

    // Create spark context  
    val sc = new SparkContext(sparkConf) 
    // Create SQLContext
    val sqlContext = new SQLContext(sc)
    // Read documents from input path
    val documents = sc.textFile(s"file:///$inputPath").map(_.split(" ").toSeq)
    
    // Convert RDD to DataFrame
   import sqlContext.implicits._
    val data = documents.zipWithIndex.map { case (words, id) => (id, words.mkString(" ")) }.toDF("label", "text")

    // Tokenize text into words
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val wordsData = tokenizer.transform(data)
    
    // Apply HashingTF to convert words into feature vectors
    val hashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("rawFeatures")
      .setNumFeatures(1000) // Adjust as needed
    val featurizedData = hashingTF.transform(wordsData)

    // Compute IDF
    val idf = new IDF()
      .setInputCol("rawFeatures")
      .setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)

    // Convert Sparse Vector to String
    val vectorToString = udf((vector: Vector) => vector.toString)

    // Add a column with the string representation of features
    val readableData = rescaledData.withColumn("featuresString", vectorToString(col("features")))

    // Show results
    readableData.select("label", "features").show(truncate = false)


    sc.stop()
  }
} 