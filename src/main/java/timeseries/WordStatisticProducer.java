package timeseries;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections.Factory;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import casdb.CassandraConn;
import casdb.CrawlStateDao;
import casdb.WordDao;
import collection.DefaultedPutMap;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import kafkastore.KafkaTopics;
import kafkastore.TweetConsumer;
import shingle.TextShingle;
import util.DateUtil;
import weibo4j.model.Status;

/**
 * 计算每个词相关的统计量，主要是计算每个词的词频序列。由于spark的kafka目前不支持0.9，暂且使用当前类来实时统计
 * 
 * @author xiafan
 *
 */
public class WordStatisticProducer extends Thread {
	private final Logger logger = Logger.getLogger(TimeSeriesProducer.class);
	CassandraConn conn;
	TweetConsumer consumer;
	WordDao wordDao;
	CrawlStateDao stateDao;
	TextShingle shingle;

	public WordStatisticProducer(String kafkaServers, String dbServers, boolean restart) {
		logger.info("connecting to cassandra");
		conn = new CassandraConn();
		conn.connect(dbServers);
		logger.info("create WordDao");
		wordDao = new WordDao(conn);
		stateDao = new CrawlStateDao(conn);
		logger.info("connecting to kafka");
		consumer = new TweetConsumer();
		consumer.open(Arrays.asList(KafkaTopics.TWEET_TOPIC, KafkaTopics.RETWEET_TOPIC), KafkaTopics.WORD_STATS_GROUP,
				kafkaServers, restart);
		shingle = new TextShingle(null);
	}

	@Override
	public void run() {
		logger.info("begin to consume tweets");
		while (true) {
			Map<String, Map<Long, Integer>> wordCounters = DefaultedPutMap
					.decorate(new HashMap<String, Map<Long, Integer>>(), new Factory() {
						@Override
						public Object create() {
							return DefaultedPutMap.decorate(new HashMap<Long, Integer>(), new Factory() {
								@Override
								public Object create() {
									return new Integer(0);
								}
							});
						}

					});
			Map<String, Map<Long, Integer>> crawlState = DefaultedPutMap
					.decorate(new HashMap<String, Map<Long, Integer>>(), new Factory() {
						@Override
						public Object create() {
							return DefaultedPutMap.decorate(new HashMap<Long, Integer>(), new Factory() {
								@Override
								public Object create() {
									return new Integer(0);
								}
							});
						}

					});
			for (Entry<String, List<Status>> topicData : consumer.nextStatus().entrySet()) {
				wordCounters.clear();
				crawlState.clear();
				for (Status cur : topicData.getValue()) {
					if (cur.getCreatedAt() != null) {
						long time = DateUtil.roundByHour(cur.getCreatedAt().getTime());
						crawlState.get(topicData.getKey()).put(time, crawlState.get(topicData.getKey()).get(time) + 1);
						try {
							List<String> words = shingle.shingling(cur.getText());
							for (String word : words) {
								Map<Long, Integer> curMap = wordCounters.get(word);
								curMap.put(time, curMap.get(time) + 1);
							}
						} catch (IOException e) {

						}
					}
				}
				for (Entry<String, Map<Long, Integer>> wordEntry : wordCounters.entrySet()) {
					for (Entry<Long, Integer> ts : wordEntry.getValue().entrySet()) {

						wordDao.updateWordFreq(wordEntry.getKey(), ts.getKey(), ts.getValue());
					}
				}
				for (Entry<String, Map<Long, Integer>> topicEntry : crawlState.entrySet()) {
					for (Entry<Long, Integer> ts : topicEntry.getValue().entrySet()) {
						stateDao.updateTopicCrawlState(topicEntry.getKey(), ts.getKey(), ts.getValue());
					}
				}
			}
		}

	}

	/**
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		PropertyConfigurator.configure("conf/log4j.properties");
		// args = new String[] { "-c", "127.0.0.1", "-k", "localhost:9092" };
		OptionParser parser = new OptionParser();
		parser.accepts("c", "cassandra server address").withRequiredArg().ofType(String.class);
		parser.accepts("k", "kafka server address").withRequiredArg().ofType(String.class);
		parser.accepts("r", "consume topics from the beginning");
		OptionSet set = parser.parse(args);
		if (!set.hasOptions()) {
			parser.printHelpOn(new PrintStream(System.out));
			System.exit(1);
		}
		WordStatisticProducer producer = new WordStatisticProducer(set.valueOf("k").toString(),
				set.valueOf("c").toString().trim(), set.has("r"));
		producer.start();
	}
}
