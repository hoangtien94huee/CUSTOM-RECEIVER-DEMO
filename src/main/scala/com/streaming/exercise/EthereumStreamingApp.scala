package com.streaming.exercise

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * EthereumStreamingApp - Spark Streaming application để theo dõi tỷ giá Ethereum
 */
object EthereumStreamingApp {
  
  def main(args: Array[String]): Unit = {

    // Disable Spark logging để output sạch hơn (đã implement sẵn)
    import org.apache.log4j.{Level, Logger}
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)
    Logger.getLogger("org.eclipse.jetty").setLevel(Level.OFF)
    Logger.getRootLogger().setLevel(Level.OFF)
    
    println("=" * 60)
    println("Starting Ethereum Streaming Application")
    println("=" * 60)
    
    // Create Spark configuration
    val conf = new SparkConf()
      .setAppName("Ethereum Streaming Application")
      .setMaster("spark://spark-master:7077")

    // Create StreamingContext with 10 second batch interval
    val ssc = new StreamingContext(conf, Seconds(10))

    val apiStream = ssc.receiverStream(
      new EthereumReceiver(
        "https://api.coinbase.com/v2/exchange-rates?currency=ETH",
        5  // fetch every 5 seconds
      )
    )

    // Process received data
    apiStream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        // Get current timestamp
        val currentTime = LocalDateTime.now()
          .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
        
        // Collect all data to avoid interleaved logs
        val collectedData = rdd.collect()
        
        // Print header
        println("\n" + "=" * 50)
        println(s"Time: $currentTime")
        println("=" * 50)
        
        collectedData.foreach { jsonData =>
          try {
            val usdRate = extractRate(jsonData, "USD")
            val vndRate = extractRate(jsonData, "VND")
            
            if (usdRate.nonEmpty && vndRate.nonEmpty) {
              println(f"ETH/USD = ${usdRate.get}%.2f , ETH/VND = ${vndRate.get}%.0f")
            }
            
          } catch {
            case e: Exception => 
              println(s"Error parsing data: ${e.getMessage}")
          }
        }
        
        println("=" * 50)
        
        // Flush output to ensure immediate display
        System.out.flush()
      }
    }

    // Start the streaming context
    println("\nStreaming context started. Waiting for data...")
    println("Press Ctrl+C to stop.\n")
    
    ssc.start()
    ssc.awaitTermination()
  }
  
  /**
   * Helper function để extract tỷ giá từ JSON (đã implement sẵn - KHÔNG CẦN SỬA)
   * 
   * @param jsonString - JSON response từ API
   * @param currency - currency code cần extract (vd: "USD", "VND")
   * @return Option[Double] - tỷ giá hoặc None nếu không tìm thấy
   * 
   * Ví dụ JSON:
   * {"data":{"currency":"ETH","rates":{"USD":"3580.25","VND":"87452000"}}}
   * 
   * extractRate(json, "USD") → Some(3580.25)
   * extractRate(json, "VND") → Some(87452000.0)
   * extractRate(json, "XYZ") → None
   */
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
