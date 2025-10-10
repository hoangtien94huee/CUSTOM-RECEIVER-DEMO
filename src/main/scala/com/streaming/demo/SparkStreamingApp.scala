package com.streaming.demo

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object SparkStreamingApp {
  def main(args: Array[String]): Unit = {

    // Tắt hoàn toàn log của Spark để chỉ hiển thị kết quả của chương trình chúng ta
    import org.apache.log4j.{Level, Logger}
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)
    Logger.getLogger("org.eclipse.jetty").setLevel(Level.OFF)
    Logger.getRootLogger().setLevel(Level.OFF)
    

    // Tạo một đối tượng cấu hình cho Spark
    val conf = new SparkConf()
      .setAppName("Ứng dụng Streaming sử dụng Custom API") // Đặt tên cho ứng dụng
      .setMaster("spark://spark-master:7077") // Đặt địa chỉ master để kết nối đến cluster Spark

    // Tạo một StreamingContext với khoảng thời gian xử lý mỗi batch là 10 giây
    val ssc = new StreamingContext(conf, Seconds(10))

    // Tạo một DStream sử dụng CustomAPIReceiver – stream dữ liệu tỷ giá Bitcoin từ Coinbase API
    val apiStream = ssc.receiverStream(new CustomAPIReceiver("https://api.coinbase.com/v2/exchange-rates?currency=BTC", 5))

    // Xử lý dữ liệu nhận được từ stream
    apiStream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        // Lấy timestamp hiện tại
        val currentTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
        
        // Thu thập toàn bộ dữ liệu để tránh log bị xen kẽ
        val collectedData = rdd.collect()
        
        // In tiêu đề thời gian
        println("\n-------------------------------------------")
        println(s"Thời gian: $currentTime")
        println("-------------------------------------------")
        
        // Xử lý từng chuỗi JSON nhận được
        collectedData.foreach { jsonData =>
          try {
            // Phân tích JSON để trích xuất tỷ giá USD và VND
            val usdRate = extractRate(jsonData, "USD")
            val vndRate = extractRate(jsonData, "VND")
            
            if (usdRate.nonEmpty && vndRate.nonEmpty) {
              println(f"BTC/USD = ${usdRate.get}%.2f , BTC/VND = ${vndRate.get}%.0f")
            }
          } catch {
            case e: Exception => 
              println(s"Lỗi khi phân tích dữ liệu: ${e.getMessage}")
          }
        }
        
        // Xả bộ đệm để đảm bảo tất cả nội dung được in ra cùng lúc
        System.out.flush()
      }
    }

    // Khởi động Spark Streaming
    ssc.start()
    // Giữ chương trình chạy cho đến khi có lệnh dừng
    ssc.awaitTermination()
  }
  
  // Hàm phụ để trích xuất tỷ giá từ chuỗi JSON
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
