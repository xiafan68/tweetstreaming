package microblog

import java.io.StringReader

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkConf
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.wltea.analyzer.core.IKSegmenter
import org.wltea.analyzer.core.Lexeme

import common.OptionParserBuilder
import kafka.serializer.DefaultDecoder
import weibo4j.model.Status
import weibo4j.org.json.JSONObject

/**
 * 1.  parse tweet to generate word pairs
 * 2. parse tweet to generate word with idx
 * 3. join word pair with tweet to generate edge
 * 4. pregal? to delete edges?
 */
object graphbasedeventmonitor {
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

    val kafkaStreams = (1 to numStreams).map(i => KafkaUtils.createStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](ssc, Map("group.id" -> group), topicMap, StorageLevel.MEMORY_ONLY))
    val unifiedStream = ssc.union(kafkaStreams)

    val statusStream = unifiedStream.map(
      line =>
        try {
          Some(new Status(new JSONObject(line._2)))
        } catch {
          case e =>
            e.printStackTrace()
            None
        }).filter(line => !line.isEmpty).map { x => x.get }

    //统计新文件中单词的次数，这里首先对每个查询中的查询词进行分词

    statusStream.foreach(
      statusRDD => {
        val words = statusRDD.mapPartitions(
          statuses => {
            var words = Set[String]()
            val segmenter = new IKSegmenter(new StringReader(""), true)
            statuses.map(status => {
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
            })
            words.iterator
          }).zipWithUniqueId()
        val wordPairs = statusRDD.mapPartitions(
          statuses => {
            var words = Set[String]()
            val segmenter = new IKSegmenter(new StringReader(""), true)
            statuses.map(status => {
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
            })
            var ret: ArrayBuffer[(String, String)] = ArrayBuffer[(String, String)]()

            for (word <- words) {
              for (wordb <- words) {
                if (word < wordb)
                  ret += ((word, wordb))
              }
            }
            ret.iterator
          })
        val nodes = words.map(x => (x._2, x._1))
        val edges = wordPairs.join(words).
          map(x => (x._2._1, x._2._2)).
          join(words).map(x => (x._2, 1)).
          reduceByKey(_ + _).map(x => Edge(x._1._1, x._1._2, x._2))
        var graph = Graph(nodes, edges, "")

        for (i <- Range(0, 10)) {
          val ccGraph = graph.connectedComponents()
          val candNodes = ccGraph.vertices.map(x => (x._2, 1)).
            reduceByKey(_ + _).filter(x => x._2 < 5).
            map(x => (x._1, ""))
          graph = Graph(graph.vertices.diff(candNodes), graph.edges, "")
        }
      })

    //算连通图，所有大小小于upper,大于lower的子图，抠出来作为一个topic，从原图中扣掉这些点，在新图上面重新计算
    ssc.checkpoint(checkpoints)
    ssc.start()
    ssc.awaitTermination()
  }
}