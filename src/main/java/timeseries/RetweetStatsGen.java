package timeseries;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import casdb.CassandraConn;
import casdb.RetweetStatsDao;
import dase.perf.MetricBasedPerfProfile;
import dase.perf.ServerController;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import kafkastore.KafkaProducerFactory;
import kafkastore.KafkaTopics;
import kafkastore.TweetConsumer;
import kafkastore.TweetKafkaProducer;
import util.DateUtil;
import weibo.Tool;
import weibo4j.model.Status;

/**
 * 为每条原始微博统计它的转发微博的相关统计信息，例如，人群信息（性别，用户类型,客户端），对于微博，
 * 
 * 
 * @author xiafan
 *
 */
public class RetweetStatsGen extends Thread implements ServerController.IServerSubscriber {
	private final Logger logger = Logger.getLogger(RetweetStatsGen.class);
	CassandraConn conn;
	TweetConsumer consumer;
	TweetKafkaProducer producer;
	RetweetStatsDao statsDao;

	Semaphore susSem = new Semaphore(1);
	ServerController controller = new ServerController(this);

	List<String> moodList = new ArrayList<String>();

	public RetweetStatsGen() {

	}

	private void loadMoodDict(String file) throws IOException {
		logger.info("loading mood dictionary:" + file);
		moodList = Tool.readFile(file, "utf-8");
	}

	public Set<String> moodsOfTweet(String content) {
		Set<String> moods = new HashSet<String>();
		for (int j = 0; j < moodList.size(); j++) {
			if (content.contains((CharSequence) moodList.get(j))) {
				if ((j >= 0) && (j <= 807)) {
					moods.add("快乐");
				} else if ((j >= 808) && (j <= 1172)) {
					moods.add("悲伤");
				} else if ((j >= 1173) && (j <= 1340)) {
					moods.add("愤怒");
				} else if ((j >= 1341) && (j <= 1515)) {
					moods.add("恐惧");
				} else if ((j >= 1516) && (j <= 1618)) {
					moods.add("惊奇");
				} else if ((j >= 1619) && (j <= 1973)) {
					moods.add("厌恶");
				} else if ((j >= 1974) && (j <= 2245)) {
					moods.add("其他");
				}
			}
		}

		return moods;
	}

	public void init(String moodFile, String kafkaServers, String dbServers, boolean restart) throws Exception {
		loadMoodDict(moodFile);
		logger.info("connecting to cassandra");
		conn = new CassandraConn();
		conn.connect(dbServers);
		logger.info("create RetweetStatsDao");
		statsDao = new RetweetStatsDao(conn);
		logger.info("connecting to kafka");
		consumer = new TweetConsumer();
		consumer.open(Arrays.asList(KafkaTopics.RETWEET_TOPIC), KafkaTopics.RTSTATS_GROUP, kafkaServers, restart);
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
					// 在高速转发的情况下，这样可以减轻写入到kafka中的状态数据
					statsDao.beginBatch();
					for (Status status : topicData.getValue()) {
						if (status.getCreatedAt() != null) {
							long tstime = DateUtil.roundByHour(status.getCreatedAt().getTime());
							String mid = status.getMid();
							if (status.getRetweetedStatus() != null) {
								mid = status.getRetweetedStatus().getMid();
							}
							if (status.getSource() != null && status.getSource().getName() != null) {
								statsDao.putClientForTweet(mid, tstime, status.getSource().getName(), 1);
							}
							if (status.getUser() != null) {
								statsDao.putVIPForTweet(mid, tstime, status.getUser().isVerified(), 1);
								String gender = status.getUser().getGender();
								if (gender == null || gender.isEmpty())
									gender = "u";
								statsDao.putGenderForTweet(mid, tstime, gender, 1);
								String loc = status.getUser().getLocation();
								if (loc == null || loc.trim().isEmpty()) {
									loc = "u";
								} else {
									String[] fields = loc.split(" ");
									if (fields.length > 0)
										loc = fields[0].trim();
									else
										loc = "u";
								}
								statsDao.putLocForTweet(mid, tstime, loc, 1);
							}
							// detect the mood of the tweets
							for (String mood : moodsOfTweet(status.getText())) {
								statsDao.putMoodForTweet(mid, tstime, mood, 1);
							}
						}
					}
					statsDao.endBatch();
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
		parser.accepts("m", "mood file").withRequiredArg().ofType(String.class);
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
		RetweetStatsGen producer = new RetweetStatsGen();
		producer.init(set.valueOf("m").toString(), set.valueOf("k").toString(), set.valueOf("c").toString().trim(),
				set.has("r"));
		producer.start();
	}
}
