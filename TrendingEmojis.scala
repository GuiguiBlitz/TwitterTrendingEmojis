import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import scala.io.Source
import scala.collection.mutable.HashMap
import java.io.File
import org.apache.log4j.Logger
import org.apache.log4j.Level

//create a new Dataframe from the saved tweets
val twitterSampleDF = sqlContext.read.parquet("sampleTweets3.parquet") 
twitterSampleDF.createOrReplaceTempView("tweetSample")

//make a RDD of all tweet Texts
val text = sqlContext.sql("SELECT text FROM tweetSample WHERE lang = 'en'").rdd.map {
  case Row(text: String) =>
    text
}

//Define a regex of the different emojis unicodes ranges
val emotRegex = "[😀-🙏]|[🌀-🗿]|[☀-⛿]|[✀-➿]" .r

//isolate all emojis, and count eah aparition
val chars = text.flatMap(content =>  emotRegex.findFirstIn(content)).map((_,1)).reduceByKey((a,b)=> a+b).sortByKey(true, 1)


//create a dataFrame of the result, to display in zeppelin
val emojiDF = sqlContext.createDataFrame(chars)
emojiDF.createOrReplaceTempView("emojiTrend")

//whith sparkSql select the result
%sql SELECT * FROM emojiTrend ORDER BY _2 DESC LIMIT 30
