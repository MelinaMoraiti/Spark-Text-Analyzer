import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.Vector

import org.apache.spark.rdd.RDD

object tfidf {

  def main(args: Array[String]): Unit = {
  
    // Create spark configuration
    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("TFIDF")

    // Create spark context  
    val sc = new SparkContext(sparkConf) 
    
    // Check if the input path is provided
    if (args.length < 1) {
      println("Usage: ./run-spark.sh <File_with_main_class> <number_of_threads> <name_of_jar_file> <fileset path>")
      System.exit(1)
    }

    // Use the input path from command line arguments
    val inputPath = args(0)

    val documents: RDD[Seq[String]] = sc.textFile(s"file:///$inputPath")
    .map(_.split(" ").toSeq)
    
    val hashingTF = new HashingTF()
    val tf: RDD[Vector] = hashingTF.transform(documents)
    
    tf.cache()
    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)

    // spark.mllib IDF implementation provides an option for ignoring terms which occur in less than
    // a minimum number of documents. In such cases, the IDF for these terms is set to 0.
    // This feature can be used by passing the minDocFreq value to the IDF constructor.
    val idfIgnore = new IDF(minDocFreq = 2).fit(tf)
    val tfidfIgnore: RDD[Vector] = idfIgnore.transform(tf)
    
    println("tfidf: ")
    tfidf.collect().foreach(x => println(x))

    println("tfidfIgnore: ")
    tfidfIgnore.collect().foreach(x => println(x))
    
    sc.stop()
  }
}