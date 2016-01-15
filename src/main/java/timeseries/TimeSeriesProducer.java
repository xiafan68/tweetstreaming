package timeseries;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import casdb.CassandraConn;
import casdb.SegStateDao;
import casdb.SegStateDao.SegState;
import casdb.TweetDao;
import dase.perf.MetricBasedPerfProfile;
import dase.perf.ServerController;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import kafkastore.KafkaProducerFactory;
import kafkastore.KafkaTopics;
import kafkastore.TimeSeriesUpdateState;
import kafkastore.TweetConsumer;
import kafkastore.TweetKafkaProducer;
import util.DateUtil;
import weibo4j.model.Status;

/**
 * 监控tweet，为它所有的父节点的转发数加1
 * 
 * @author xiafan
 *
 */
public class TimeSeriesProducer extends Thread implements ServerController.IServerSubscriber {
	private final Logger logger = Logger.getLogger(TimeSeriesProducer.class);
	CassandraConn conn;
	TweetConsumer consumer;
	TweetKafkaProducer producer;
	TweetDao tweetDao;
	SegStateDao segDao;
	Random rand = new Random();
	Semaphore susSem = new Semaphore(1);
	ServerController controller = new ServerController(this);

	public TimeSeriesProducer() {

	}

	public void init(String kafkaServers, String dbServers, boolean restart) throws Exception {
		logger.info("connecting to cassandra");
		conn = new CassandraConn();
		conn.connect(dbServers);
		logger.info("create TweetDao");
		tweetDao = new TweetDao(conn);
		segDao = new SegStateDao(conn);
		logger.info("connecting to kafka");
		consumer = new TweetConsumer();
		consumer.open(Arrays.asList(KafkaTopics.RETWEET_TOPIC), KafkaTopics.RTSERIES_GROUP, kafkaServers, restart);
		producer = new TweetKafkaProducer(KafkaProducerFactory.createProducer(kafkaServers));
		MetricBasedPerfProfile.registerServer(controller);
	}

	@Override
	public void run() {
		logger.info("begin to consume tweets");
		controller.running();
		while (true) {
			susSem.acquireUninterruptibly();
			try {
				for (Entry<String, List<Status>> topicData : consumer.nextStatus().entrySet()) {
					try {
						// 在高速转发的情况下，这样可以减轻写入到kafka中的状态数据
						Map<String, TimeSeriesUpdateState> states = new HashMap<String, TimeSeriesUpdateState>();
						List<TimeSeriesUpdateState> segSignal = new ArrayList<TimeSeriesUpdateState>();
						for (Status cur : topicData.getValue()) {
							tweetDao.putTweet(cur);
							if (cur.getRetweetedStatus() != null) {
								if (rand.nextFloat() < 0.01
										&& tweetDao.getStatusByMid(cur.getRetweetedStatus().getMid()) == null) {

								}
								tweetDao.putRtweet(cur);
								for (TimeSeriesUpdateState state : tweetDao.updateRtTimeSeries(cur)) {
									states.put(state.getMid(), state);
								}
							} else {
								// maybe it is the first tweet or an indication
								// for end of monitoring
								if (cur.getMid() != null) {
									SegState state = segDao.getSegState(cur.getMid());
									if (state != null) {
										long updateDate = DateUtil.roundByHour(System.currentTimeMillis());
										segSignal.add(new TimeSeriesUpdateState(state.mid, updateDate, true));
										List<String> mids = tweetDao.getRtMids(state.mid);
										for (String rtMid : mids) {
											segSignal.add(new TimeSeriesUpdateState(rtMid, updateDate, true));
										}
									}
								}
							}
						}
						for (TimeSeriesUpdateState state : segSignal) {
							states.put(state.mid, state);
						}
						for (TimeSeriesUpdateState state : states.values()) {
							logger.info("update time series " + state);
							producer.storeTsUpdateState(state);
						}
					} catch (Exception ex) {
						ex.printStackTrace();
					}
				}
			} finally {
				susSem.release();
			}
		}
	}

	@Override
	public void onStop() {
		if (conn != null) {
			conn.close();
			conn = null;
			producer.close();
			consumer.close();
		}
	}

	@Override
	public void onSuspend() {
		susSem.acquireUninterruptibly();
	}

	@Override
	public void onResume() {
		susSem.release();
	}

	/**
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		PropertyConfigurator.configure("conf/log4j.properties");
		// args = new String[] { "-c", "127.0.0.1", "-k", "localhost:9092" };
		OptionParser parser = new OptionParser();
		parser.accepts("c", "cassandra server address").withRequiredArg().ofType(String.class);
		parser.accepts("k", "kafka server address").withRequiredArg().ofType(String.class);
		parser.accepts("r", "consume topics from the beginning");
		parser.accepts("g", "server address of ganglia gmond. e.g 10.11.1.212:8649").withRequiredArg()
				.ofType(String.class);
		OptionSet set = parser.parse(args);
		if (!set.hasOptions()) {
			parser.printHelpOn(new PrintStream(System.out));
			System.exit(1);
		}

		String[] fields = set.valueOf("g").toString().split(":");
		MetricBasedPerfProfile.reportForGanglia(fields[0], Short.parseShort(fields[1].trim()));
		TimeSeriesProducer producer = new TimeSeriesProducer();
		producer.init(set.valueOf("k").toString(), set.valueOf("c").toString().trim(), set.has("r"));
		producer.start();
	}
}
