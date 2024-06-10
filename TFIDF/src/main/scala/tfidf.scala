import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.sql.functions._

object tfidf {
  def main(args: Array[String]): Unit = {
    // Ελεγχος αν ο κατάλογος εισόδου έχει δοθεί ως όρισμα απο τη γραμμή εντολών
    if (args.length < 1) {
      println("Usage: ./run-spark.sh <File_with_main_class> <number_of_threads> <name_of_jar_file> <fileset path>")
      System.exit(1)
    }
    // Χρηση της διαδρομής εισόδου από τα ορίσματα γραμμής εντολών
    val inputPath = args(0)
    // Spark Setup
    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("TFIDF")
    // Δημιουρία του περιβάλλοντος εκτέλεσης του Spark
    val sc = new SparkContext(sparkConf) 
    // Δημιουργία του περιβάλλοντος SQL
    val sqlContext = new SQLContext(sc)
    // Διαβάζει τα αρχεία απο τον κατάλογο απο τη διαδρομή που δόθηκε ως είσοδος
    val documents = sc.textFile(s"file:///$inputPath").map(_.split(" ").toSeq)
    // Μετατροπή του RDD σε DataFrame
    import sqlContext.implicits._
    val data = documents.zipWithIndex.map { case (words, id) => (id, words.mkString(" ")) }.toDF("label", "file_contend")
    // Διαχωρίζει το κείμενο κάθε αρχείου  σε tokens (΄΄ορους)
    val tokenizer = new Tokenizer().setInputCol("file_content").setOutputCol("words")
    val wordsData = tokenizer.transform(data)
    // Το HashingTF μετατρέπει τα tokens σε σε διανύσματα χαρακτηριστικών
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(12) 
    val featurizedData = hashingTF.transform(wordsData)
    // Υπολογισμός του IDF
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)
    // Εμφάνιση όλων των αποτελεσμάτων χωρίς περικοπή
    rescaledData.select("label", "features").show(truncate = false)
    sc.stop()
  }
}
