package com.streaming.demo

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object SparkStreamingApp {
  def main(args: Array[String]): Unit = {

    // Completely disable Spark logging to show only our output
    import org.apache.log4j.{Level, Logger}
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)
    Logger.getLogger("org.eclipse.jetty").setLevel(Level.OFF)
    Logger.getRootLogger().setLevel(Level.OFF)
    
    // Also set Spark context log level
    val sparkContext = org.apache.spark.SparkContext.getOrCreate()
    sparkContext.setLogLevel("OFF")
    
    // Create a Spark configuration object
    val conf = new SparkConf()
      .setAppName("Custom API Streaming Application") // Set the application name
      .setMaster("spark://spark-master:7077") // Set the master URL to connect to the Spark cluster

    // Create a StreamingContext with a batch interval of 10 seconds
    val ssc = new StreamingContext(conf, Seconds(10))

    // Create a DStream using the CustomAPIReceiver - streaming Bitcoin exchange rates from Coinbase
    val apiStream = ssc.receiverStream(new CustomAPIReceiver("https://api.coinbase.com/v2/exchange-rates?currency=BTC", 5))

    // Process the received data
    apiStream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        // Get current timestamp first
        val currentTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
        
        // Collect all data first to avoid interleaved logs
        val collectedData = rdd.collect()
        
        // Print time header
        println("\n-------------------------------------------")
        println(s"Time: $currentTime")
        println("-------------------------------------------")
        
        // Process each JSON response
        collectedData.foreach { jsonData =>
          try {
            // Parse JSON to extract USD and VND rates
            val usdRate = extractRate(jsonData, "USD")
            val vndRate = extractRate(jsonData, "VND")
            
            if (usdRate.nonEmpty && vndRate.nonEmpty) {
              println(f"BTC/USD = ${usdRate.get}%.2f , BTC/VND = ${vndRate.get}%.0f")
            }
          } catch {
            case e: Exception => 
              println(s"Error parsing data: ${e.getMessage}")
          }
        }
        
        // Flush output to ensure it appears together
        System.out.flush()
      }
    }

    // Start the streaming context
    ssc.start()
    // Await termination of the streaming context
    ssc.awaitTermination()
  }
  
  // Helper function to extract exchange rate from JSON string
  def extractRate(jsonString: String, currency: String): Option[Double] = {
    try {
      val pattern = s""""$currency":"([^"]+)"""".r
      pattern.findFirstMatchIn(jsonString) match {
        case Some(matched) => Some(matched.group(1).toDouble)
        case None => None
      }
    } catch {
      case _: Exception => None
    }
  }
}