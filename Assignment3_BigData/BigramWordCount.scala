/**
  * Created by aniru on 11/16/2016.
  */

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils


import scala.collection.JavaConversions._
import scala.collection.immutable.ListMap
import java.util.Properties
import scala.collection.mutable.ArrayBuffer
import stemmer.Stemmer
object BigramWordCount {
  def main(args: Array[String]) {
    val stemmer = new Stemmer
    val (zkQuorum, group, topics, numThreads) = ("localhost", "localhost", "mytopicBigramCount", "20")
    val sparkConf = new SparkConf().setMaster("local[*]").setSparkHome("C:\\Users\\aniru\\Desktop\\spark_test\\spark-1.6.0-bin-hadoop2.6\\bin").setAppName("BigramWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint("checkpoint")

    val pattern = "(\\ba\\b|\\bable\\b|\\babout\\b|\\bacross\\b|\\bafter\\b|\\ball\\b|\\balmost\\b|\\balso\\b|\\bam\\b|\\bamong\\b|\\ban\\b|\\band\\b|\\bany\\b|\\bare\\b|\\bas\\b|\\bat\\b|\\bbe\\b|\\bbecause\\b|\\bbeen\\b|\\bbut\\b|\\bby\\b|\\bcan\\b|\\bcannot\\b|\\bcould\\b|\\bdear\\b|\\bdid\\b|\\bdo\\b|\\bdoes\\b|\\beither\\b|\\belse\\b|\\bever\\b|\\bevery\\b|\\bfor\\b|\\bfrom\\b|\\bget\\b|\\bgot\\b|\\bhad\\b|\\bhas\\b|\\bhave\\b|\\bhe\\b|\\bher\\b|\\bhers\\b|\\bhim\\b|\\bhis\\b|\\bhow\\b|\\bhowever\\b|\\bi\\b|\\bif\\b|\\bin\\b|\\binto\\b|\\bis\\b|\\bit\\b|\\bits\\b|\\bjust\\b|\\bleast\\b|\\blet\\b|\\blike\\b|\\blikely\\b|\\bmay\\b|\\bme\\b|\\bmight\\b|\\bmost\\b|\\bmust\\b|\\bmy\\b|\\bneither\\b|\\bno\\b|\\bnor\\b|\\bnot\\b|\\bof\\b|\\boff\\b|\\boften\\b|\\bon\\b|\\bonly\\b|\\bor\\b|\\bother\\b|\\bour\\b|\\bown\\b|\\brather\\b|\\bsaid\\b|\\bsay\\b|\\bsays\\b|\\bshe\\b|\\bshould\\b|\\bsince\\b|\\bso\\b|\\bsome\\b|\\bthan\\b|\\bthat\\b|\\bthe\\b|\\btheir\\b|\\bthem\\b|\\bthen\\b|\\bthere\\b|\\bthese\\b|\\bthey\\b|\\bthis\\b|\\btis\\b|\\bto\\b|\\btoo\\b|\\btwas\\b|\\bus\\b|\\bwants\\b|\\bwas\\b|\\bwe\\b|\\bwere\\b|\\bwhat\\b|\\bwhen\\b|\\bwhere\\b|\\bwhich\\b|\\bwhile\\b|\\bwho\\b|\\bwhom\\b|\\bwhy\\b|\\bwill\\b|\\bwith\\b|\\bwould\\b|\\byet\\b|\\byou\\b|\\byour\\b)".r

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    val wordsList = lines.map{

      //split elements by periods
      _.split('.').map{ subElements =>

        // tokenize on spaces
        subElements.trim.split(' ').

          // Remove non-alphanumeric and change the words to lowercase
          map{_.replaceAll("""\W""", "").toLowerCase()}. map(word=>pattern.replaceAllIn(word, "be")).map(re=>{stemmer.add(re.toArray,re.length)
          stemmer.stem()
          stemmer.toString}).

          // check for bigrams
          sliding(2)
      }.

        // Flatten, and map the bigrams to concatenated strings
        flatMap{identity}.map{_.mkString(" ")}.

        // Group the bigrams and count their frequency
        groupBy{identity}.mapValues{_.size}

    }. flatMap{identity}.reduceByKey(_+_)
    val outputBigram = wordsList.filter(a=>a._2>1)

    outputBigram.print()


    ssc.start()
    ssc.awaitTermination()
  }
}
