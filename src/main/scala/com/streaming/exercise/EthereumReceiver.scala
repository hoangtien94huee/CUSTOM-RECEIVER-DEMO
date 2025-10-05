package com.streaming.exercise

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.Source
import scala.util.{Failure, Success, Try}

/**
 * EthereumReceiver - Custom Receiver để fetch tỷ giá Ethereum
 * 
 * Hoàn thành các phương thức có TODO
 */
class EthereumReceiver(apiUrl: String, intervalSeconds: Int) 
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  @volatile private var fetchingData: Boolean = false

  /**
   * TODO 1: Implement phương thức onStart()
   * 
   * Yêu cầu:
   * - Set fetchingData = true
   * - Tạo Future để fetch data liên tục trong background
   * - Trong loop: gọi fetchWithRetry(), store data, sleep theo interval
   * 
   * Gợi ý cấu trúc:
   * fetchingData = true
   * Future {
   *   while (fetchingData) {
   *     val data = fetchWithRetry()
   *     data.foreach(store)
   *     Thread.sleep(intervalSeconds * 1000)
   *   }
   * }
   */
  override def onStart(): Unit = {
    // TODO: Viết code của bạn ở đây
    ???
  }

  /**
   * TODO 2: Implement phương thức onStop()
   * Yêu cầu:
   * - Dừng việc fetch data khi receiver stop
   * 
   * Gợi ý: Set fetchingData = false
   */
  override def onStop(): Unit = {
    // TODO: Viết code của bạn ở đây
    ???
  }

  /**
   * TODO 3: Implement phương thức fetchWithRetry() (4 điểm)
   * 
   * Yêu cầu:
   * - Thử fetch data từ API
   * - Nếu thất bại, retry tối đa maxRetries lần
   * - Mỗi lần retry, delay tăng gấp đôi (exponential backoff)
   * - Log rõ ràng mỗi bước (attempt, retry, success, failure)
   * 
   * Parameters:
   * @param maxRetries - số lần retry tối đa (mặc định 3)
   * @param initialDelay - delay ban đầu tính bằng giây (mặc định 2)
   * @return Option[String] - dữ liệu fetch được hoặc None nếu fail hết
   * 
   * Gợi ý:
   * - Sử dụng đệ quy với inner function attempt()
   * - Base case: remainingRetries == 0 hoặc API success
   * - Recursive case: API fail → sleep → retry với delay * 2
   * 
   * Ví dụ exponential backoff:
   * - Lần 1: delay 2 giây
   * - Lần 2: delay 4 giây  
   * - Lần 3: delay 8 giây
   */
  private def fetchWithRetry(maxRetries: Int = 3, initialDelay: Int = 2): Option[String] = {
    
    def attempt(remainingRetries: Int, currentDelay: Int): Option[String] = {
      // TODO: Implement logic ở đây
      
      // Gợi ý các bước:
      // 1. Log attempt number: println(s"Attempting to fetch data...")
      // 2. Gọi fetchDataFromAPI() để thử fetch
      // 3. Pattern match kết quả:
      //    - Success → log success và return data
      //    - Failure → 
      //      * Nếu remainingRetries > 0:
      //        - Log retry message
      //        - Sleep(currentDelay * 1000)
      //        - Gọi đệ quy: attempt(remainingRetries - 1, currentDelay * 2)
      //      * Nếu remainingRetries == 0:
      //        - Log "all retries exhausted"
      //        - Return None
      
      ???
    }
    
    // Bắt đầu retry process
    attempt(maxRetries, initialDelay)
  }

  /**
   * Fetch data từ API
   * 
   * @return Option[String] - JSON response hoặc None nếu có lỗi
   */
  private def fetchDataFromAPI(): Option[String] = {
    Try {
      val response = Source.fromURL(apiUrl).mkString
      Some(response)
    } match {
      case Success(data) => data
      case Failure(exception) =>
        println(s"Error fetching data: ${exception.getMessage}")
        None
    }
  }
}
