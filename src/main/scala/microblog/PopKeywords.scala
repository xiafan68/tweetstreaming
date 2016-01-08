package microblog

import org.apache.spark.SparkConf
import kafka.serializer.DefaultDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import common.OptionParserBuilder
import org.wltea.analyzer.core.Lexeme
import org.wltea.analyzer.core.IKSegmenter
import xiafan.util.DateUtil
import java.io.StringReader
import weibo4j.org.json.JSONObject
import weibo4j.model.Status
import org.apache.commons.codec.StringDecoder

object PopKeywords {
  case class WordZScore(word: String, zscore: Double, timestamp: Long)
  def usage(): Unit = System.err.println("""
StreamingExample Usage: 
--input inputdir 
--stopwords stopfile 
--check checkfile
--output outputdir
--sqlport port
sqlport: the jdbc port to provide service
""")

  def main(args: Array[String]) {
    val parser = new OptionParserBuilder().
      withOptions(List("zkQuorum", "group", "topics", "numThreads", "stopwords", "checkpoint")).
      withOTypes(Map('zkquorum -> "o", 'group -> "o", 'topics -> "o", 'numThreads -> "o", 'stopwords -> "o", 'checkpoint -> "o")).
      buildInstance()
    val options = parser.nextOption(args.toList)
    val (zkQuorum, group, topics, numThreads, stopwords, checkpoints) = (options('zkquorum).toString(),
      options('group).toString(),
      options('topics).toString(),
      options('numThreads).toString().toInt,
      options('stopwords).toString(),
      options('checkpoint).toString())
    // Create the context
    val conf = new SparkConf()
    conf.setAppName("hdfswordcount")
    val ssc = new StreamingContext(conf, Seconds(60 * 60))

    //加载停用词，有兴趣的同学可以考虑使用broadcast变量来实现一下
    val sc = ssc.sparkContext
    val stopWords = sc.broadcast(sc.textFile(stopwords).collect().toSet)

    // 创建一个FileInputDStream监控给定的目录
    val numStreams = 5
    val topicMap = topics.split(",").map((_, numThreads)).toMap
    //val kafkaStreams = (1 to numStreams).map(i => KafkaUtils.createStream(ssc, zkQuorum, group, topicMap))

    val kafkaStreams = (1 to numStreams).map(i => KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, Map("group.id" -> group), topicMap, StorageLevel.MEMORY_ONLY))
    val unifiedStream = ssc.union(kafkaStreams)

    //统计新文件中单词的次数，这里首先对每个查询中的查询词进行分词
    val words = unifiedStream.mapPartitions(
      lines => {
        val segmenter = new IKSegmenter(new StringReader(""), true)
        lines.map(line => {
          var status: Status = null
          try {
            status = new Status(new JSONObject(line._2))
          } catch {
            case e =>
              e.printStackTrace()
              return
          }
          var words = Set[String]()
          segmenter.reset(new StringReader(status.getText))
          var token: Lexeme = null
          var hasNext = true
          while (hasNext) {
            token = segmenter.next()
            if (token != null && !stopwords.contains(token.getLexemeText)) {
              words += token.getLexemeText()
            } else {
              hasNext = false
            }
          }
          (DateUtil.roundByHour(status.getCreatedAt.getTime), words.toArray)
        })
      }).flatMap { x => x._2.map(word => ((word, x._1), 1)) }.reduceByKey(_ + _)

    val wordsStats = words.map(x => (x._1._1, x._2)).updateStateByKey((k: Seq[Int], v: Option[(Int, Int, Int)]) => {
      val curSum = k.sum
      var sqSum = 0
      k.foreach(x => sqSum += x * x)
      var ret: Option[(Int, Int, Int)] = null
      if (v.isEmpty) {
        ret = Some[(Int, Int, Int)]((curSum, sqSum, 1))
      } else {
        ret = Some[(Int, Int, Int)]((curSum + v.get._1, sqSum + v.get._2, v.get._2))
      }
      ret
    })

    val wordZScore = words.map(x => (x._1._1, (x._1._2, x._2))).join(wordsStats).map {
      rec =>
        {
          val (word, ((timeStamp: Long, freq: Int), (sum: Int, sqSum: Int, count: Int))) = rec
          val avg = sum.toFloat / count
          val varience = scala.math.sqrt(sqSum.toFloat / count - scala.math.pow(avg, 2))
          (word, (freq - avg) / varience, timeStamp)
        }
    }

    val sqlContext = new SQLContext(ssc.sparkContext)
    import sqlContext.implicits._
    wordZScore.filter(x => scala.math.abs(x._2) > 1.0).mapPartitions[Int] { recs =>
      {
        val ret: List[Int] = Nil
        val dao = WordZScoreDao.create("127.0.0.1")
        recs.foreach(
          x => dao.store(x._1.toString(), x._2, x._3))
        dao.close()
        ret.iterator
      }
    }

    ssc.checkpoint(checkpoints)
    ssc.start()
    ssc.awaitTermination()
  }
}