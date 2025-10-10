// CustomAPIReceiver.scala
package com.streaming.demo

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.Source
import scala.util.{Failure, Success, Try}

/**
 * CustomAPIReceiver là một Spark Streaming Receiver dùng để lấy dữ liệu từ REST API công khai.
 * Nó kế thừa lớp Receiver và triển khai các phương thức onStart và onStop.
 *
 * @param apiUrl Đường dẫn (URL) của REST API để lấy dữ liệu.
 * @param intervalSeconds Khoảng thời gian (tính bằng giây) giữa các lần lấy dữ liệu từ API.
 */
class CustomAPIReceiver(apiUrl: String, intervalSeconds: Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  @volatile private var fetchingData: Boolean = false

  /**
   * Bắt đầu receiver bằng cách tạo một luồng (thread) để liên tục lấy dữ liệu từ API.
   */
  override def onStart(): Unit = {
    fetchingData = true
    Future {
      while (fetchingData) {
        val data = fetchDataFromAPI()
        data.foreach(store) // Lưu dữ liệu lấy được vào Spark
        Thread.sleep(intervalSeconds * 1000) // Chờ trong khoảng thời gian được chỉ định
      }
    }
  }

  /**
   * Dừng receiver bằng cách dừng luồng lấy dữ liệu.
   */
  override def onStop(): Unit = {
    fetchingData = false
  }

  /**
   * Lấy dữ liệu từ API được chỉ định.
   *
   * @return Một Option chứa dữ liệu lấy được dưới dạng chuỗi (String).
   */
  private def fetchDataFromAPI(): Option[String] = {
    Try {
      val response = Source.fromURL(apiUrl).mkString
      Some(response)
    } match {
      case Success(data) => data
      case Failure(exception) =>
        println(s"Lỗi khi lấy dữ liệu: ${exception.getMessage}")
        None
    }
  }
}