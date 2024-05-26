import org.apache.spark.{SparkConf, SparkContext}

object maxtermfrequency {

  def main(args: Array[String]): Unit = {
    
    println("***************************************************")
    println("***************************************************")
    
    println("Hi, this is the MaxTermFrequency application for Spark")
   
    // Create spark configuration
    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("MaxTermFrequency")

    // Create spark context  
    val sc = new SparkContext(sparkConf) 

    // Check if the input path is provided
    if (args.length < 1) {
      println("Usage: ./run-spark.sh <File_with_main_class> <number_of_threads> <name_of_jar_file> <fileset path>")
      System.exit(1)
    }

    // Use the input path from command line arguments
    val inputPath = args(0)
    val txtFiles = sc.wholeTextFiles(s"file:///$inputPath", 4)

    // Map each file to its content
    val map_by_file = txtFiles.map(file_text => (file_text._1, file_text._2))

    // Step 1: Calculate term frequency, total words in doc, and initialize document count (number of files a term exists one or more times)
    val termFreqAndDocCount = map_by_file
      .flatMap(file_words => {
        val words = file_words._2.split("\\s+") // Split content using whitespaces into terms 
        val wordCount = words.length // Total words in the document
        val wordFrequencyMap = words.groupBy(identity).mapValues(_.length) // Count occurrences of each word by grouping identical words
        // Create tuples ((word, document), (normalized frequency, document count initialized to 1))
        wordFrequencyMap.map { case (word, frequency) => ((word, file_words._1), (frequency.toDouble / wordCount, 1)) }
      })
      .reduceByKey { case ((freq1, docCount1), (freq2, docCount2)) => 
        (freq1 + freq2, docCount1 + docCount2) // Aggregate term frequencies and document counts
      }

    // Step 2: Find the maximum normalized term frequency for each word
    val maxTermFreqPerWord = termFreqAndDocCount
      .map { case ((word, docname), (normalizedFreq, docCount)) =>
        (word, (docname, normalizedFreq, docCount)) // Transform to (word, (document, normalized frequency, document count))
      }
      .reduceByKey { case ((docname1, freq1, docCount1), (docname2, freq2, docCount2)) =>
        if (freq1 > freq2) (docname1, freq1, docCount1) else (docname2, freq2, docCount2) // Keep the values with the highest normalized frequency
      }

    // Step 3: Sum up the document counts for each word
    val docCountPerWord = map_by_file
      .flatMap { case (docname, content) =>
        content.split("\\s+").distinct.map(word => (word, 1)) // For each word in the document, map to (word, 1) for counting distinct words across documents
      }
      .reduceByKey(_ + _) // Sum the document counts for each word

    // Print the header for the output
    println("((term,filename), (maximum term frequency, number of docs that word exists)")

    // Step 4: Join maxTermFreqPerWord with docCountPerWord to get the final output
    val finalResult = maxTermFreqPerWord
      .join(docCountPerWord) // Join on the word to get ((word, document), (max normalized frequency, document count))
      .map { case (word, ((docname, maxNormalizedFreq, _), docCount)) =>
        // Extract just the filename from the full path
        val filename = docname.split("/").last
        ((word, filename), (maxNormalizedFreq, docCount)) // Transform to desired output format
      }
      .collect() // Collect the result to the driver
      .foreach(println) // Print each result

    sc.stop()

    println("***************************************************")
    println("***************************************************")
  }
}
