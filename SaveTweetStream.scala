import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.{Seconds,StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.twitter._
import org.apache.spark.storage.StorageLevel
import scala.io.Source
import scala.collection.mutable.HashMap
import java.io.File
import org.apache.log4j.Logger
import org.apache.log4j.Level
import sys.process.stringSeqToProcess

import org.apache.spark.streaming.twitter._



/** Configures the Oauth Credentials for accessing Twitter */
def configureTwitterCredentials(apiKey: String, apiSecret: String, accessToken: String, accessTokenSecret: String) {
  val configs = new HashMap[String, String] ++= Seq(
    "apiKey" -> apiKey, "apiSecret" -> apiSecret, "accessToken" -> accessToken, "accessTokenSecret" -> accessTokenSecret)
  println("Configuring Twitter OAuth")
  configs.foreach{ case(key, value) =>
    if (value.trim.isEmpty) {
      throw new Exception("Error setting authentication - value for " + key + " not set")
    }
    val fullKey = "twitter4j.oauth." + key.replace("api", "consumer")
    System.setProperty(fullKey, value.trim)
    println("\tProperty " + fullKey + " set as [" + value.trim + "]")
  }
  println()
}

// Configure Twitter credentials
val apiKey = "XXXXXXXXX"
val apiSecret = "XXXXXXXX"
val accessToken = "XXXXXXXXX"
val accessTokenSecret = "XXXXXXXXX"
configureTwitterCredentials(apiKey, apiSecret, accessToken, accessTokenSecret)


val ssc = new StreamingContext(sc, Seconds(2))
val tweets = TwitterUtils.createStream(ssc, None)
val twt = tweets.window(Seconds(1800))

case class Tweet(userName:String,text:String,lang:String,location:String)
twt.map(status=>
  Tweet(status.getUser.getName(),status.getText(),status.getUser.getLang(),status.getUser.getLocation())
).foreachRDD(rdd=>
  rdd.toDF().registerTempTable("tweetTable")
)


//Start reading the stream
ssc.start()

tweets.start()



//once you have collected enough data, save it in a parquet
val data = sqlContext.sql("SELECT * FROM tweetTable ").write.format("parquet").save("sampleTweets3.parquet")

//Strop the stream
ssc.stop(stopSparkContext=false, stopGracefully=true)
