// CustomAPIReceiver.scala
package com.streaming.demo

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.Source
import scala.util.{Failure, Success, Try}

/**
 * CustomAPIReceiver is a Spark Streaming Receiver that fetches data from a public REST API.
 * It extends the Receiver class and implements the onStart and onStop methods.
 *
 * @param apiUrl The URL of the REST API to fetch data from.
 * @param intervalSeconds The interval in seconds to fetch data from the API.
 */
class CustomAPIReceiver(apiUrl: String, intervalSeconds: Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  @volatile private var fetchingData: Boolean = false

  /**
   * Starts the receiver by creating a thread that fetches data from the API.
   */
  override def onStart(): Unit = {
    fetchingData = true
    Future {
      while (fetchingData) {
        val data = fetchDataFromAPI()
        data.foreach(store) // Store the fetched data in Spark
        Thread.sleep(intervalSeconds * 1000) // Wait for the specified interval
      }
    }
  }

  /**
   * Stops the receiver by stopping the data fetching thread.
   */
  override def onStop(): Unit = {
    fetchingData = false
  }

  /**
   * Fetches data from the specified API URL.
   *
   * @return An Option containing the fetched data as a String.
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