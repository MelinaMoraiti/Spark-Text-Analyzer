import org.apache.spark.{SparkConf, SparkContext}

object maxtermfrequency {

  def main(args: Array[String]): Unit = {
    
    println("***************************************************")
    println("***************************************************")
    
    println("Hi, this is the MaxTermFrequency application for Spark")
   
    // Δημιουργία ρυθμίσεων για το Spark
    val sparkConf = new SparkConf()
      .setMaster("local[2]") //Ορίζει τον τοπικό "master" όπου το "[2]" υποδεικνύει τον αριθμό των νημάτων που θα χρησιμοποιηθούν για την επεξεργασία του Spark
      .setAppName("MaxTermFrequency") // ορίζει το όνομα της εφαρμογής

    // Δημιουργία spark context  
    val sc = new SparkContext(sparkConf) 

    // Έλεγχος εάν έχει δοθεί η διαδρομή εισόδου για τον κατάλογο με το σύνολο αρχείων, απο τα ορίσματα της γραμμής εντολών
    if (args.length < 1) {
      println("Usage: ./run-spark.sh <File_with_main_class> <number_of_threads> <name_of_jar_file> <fileset path>")
      System.exit(1)
    }

    // Χρήση της διαδρομής εισόδου πρώτη παράμετρος της γραμμής εντολών
    val inputPath = args(0)
    val txtFiles = sc.wholeTextFiles(s"file:///$inputPath", 4)

    // Αντιστοίχιση κάθε αρχείου με το περιεχόμενό του
    val map_by_file = txtFiles.map(file_text => (file_text._1, file_text._2))
// Βήμα 1: Υπολογισμός συχνότητας όρων, αριθμός όρων στο αρχείο και αρχικοποίηση του αριθμού αρχείων (αριθμός αρχείων που ένας όρος υπάρχει μία ή περισσότερες φορές)
    val termFreqAndDocCount = map_by_file
       .flatMap(file_words => {
        val words = file_words._2.split("\\s+") // Χωρίζει το περιεχομένου του αρχείου σε λέξεις χρησιμοποιώντας ως διαχωριστικό τα κενά
        val wordCount = words.length // Συνολικές λέξεις στο αρχείο
        val wordFrequencyMap = words.groupBy(identity).mapValues(_.length) // Ομαδοποιεί τις ίδιες λέξεις προκειμένου να μετρήσει τη συχνότητα κάθεμίας στο αρχείο
        // Δημιουργία ζευγαριών ((λέξη, έγγραφο), (κανονικοποιημένη συχνότητα, αρχικοποιημένος μετρητής εγγράφων σε 1))
        wordFrequencyMap.map { case (word, frequency) => ((word, file_words._1), (frequency.toDouble / wordCount, 1)) }
      })
      .reduceByKey { case ((freq1, docCount1), (freq2, docCount2)) => 
        (freq1 + freq2, docCount1 + docCount2) // Συνολική συχνότητα όρων και μετρητής αρχείων
      }

    // Βήμα 2: Εύρεση της μέγιστης κανονικοποιημένης συχνότητας για κάθε λέξη
    val maxTermFreqPerWord = termFreqAndDocCount
      .map { case ((word, docname), (normalizedFreq, docCount)) =>
        (word, (docname, normalizedFreq, docCount)) // Μετατροπή σε (λέξη, (έγγραφο, κανονικοποιημένη συχνότητα, μετρητής εγγράφων))
      }
      .reduceByKey { case ((docname1, freq1, docCount1), (docname2, freq2, docCount2)) =>
        if (freq1 > freq2) (docname1, freq1, docCount1) else (docname2, freq2, docCount2) // "Κρατάει" τις τιμές με την υψηλότερη κανονικοποιημένη συχνότητα
      }

    // Βήμα 3: Άθροισμα του αριθμού των αρχείων για κάθε λέξη
    val docCountPerWord = map_by_file
      .flatMap { case (docname, content) =>
        content.split("\\s+").distinct.map(word => (word, 1)) // Για κάθε λέξη στο έγγραφο, αντιστοίζεται με (λέξη, 1) για να μετρηθούν οι διαφορετικές λέξεις σε κάθε αρχείο
      }
      .reduceByKey(_ + _) // Sum the document counts for each word

    println("((term,filename), (maximum term frequency, number of docs that word exists)")

    // Βήμα 4: Ένωση του maxTermFreqPerWord με docCountPerWord για το τελικό αποτέλεσμα
    val finalResult = maxTermFreqPerWord
      .join(docCountPerWord)  // Ένωση με βάση τη λέξη για έχουμε ζεύγη ((λέξη, έγγραφο), (μέγιστη κανονικοποιημένη συχνότητα, μετρητής εγγράφων))
      .map { case (word, ((docname, maxNormalizedFreq, _), docCount)) =>
        // Εξαγωγή μόνο του ονόματος αρχείου από την πλήρη διαδρομή
        val filename = docname.split("/").last
        ((word, filename), (maxNormalizedFreq, docCount)) // Transform to desired output format
      }
      .collect() 
      .foreach(println) // Εκτύπωση κάθε αποτελέσματος

    sc.stop()

    println("***************************************************")
    println("***************************************************")
  }
}
