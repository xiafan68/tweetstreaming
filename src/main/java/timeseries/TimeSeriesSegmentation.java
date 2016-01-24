package timeseries;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.codahale.metrics.Timer.Context;

import casdb.CassandraConn;
import casdb.SegStateDao;
import casdb.SegStateDao.SegState;
import casdb.TweetDao;
import dase.perf.MetricBasedPerfProfile;
import dase.perf.ServerController;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import kafkastore.KafkaTopics;
import kafkastore.TimeSeriesUpdateState;
import kafkastore.TweetConsumer;
import searchapi.TweetSeg;
import searchapi.TweetService;
import segmentation.ISegmentation.ISegSubscriber;
import segmentation.Interval;
import segmentation.SWSegmentation;
import segmentation.Segment;
import util.DateUtil;

/**
 * 
 * @author xiafan
 *
 */
public class TimeSeriesSegmentation implements ServerController.IServerSubscriber {
	private final Logger logger = Logger.getLogger(TimeSeriesSegmentation.class);
	CassandraConn conn;
	TweetDao tweetDao;
	SegStateDao segDao;
	TweetConsumer consumer;

	TTransport transport;
	TweetService.Client client;
	String kafkaServer;
	String dbServer;
	String indexServer;

	Semaphore susSem = new Semaphore(1);
	ServerController controler = new ServerController(this);

	public TimeSeriesSegmentation(String kafkaServer, String dbServer, String indexServer) {
		super();
		this.kafkaServer = kafkaServer;
		this.dbServer = dbServer;
		this.indexServer = indexServer;
	}

	public void init(boolean restart) throws Exception {
		connectToLSMOIndex();
		conn = new CassandraConn();
		conn.connect(dbServer);
		tweetDao = new TweetDao(conn);
		segDao = new SegStateDao(conn);
		consumer = new TweetConsumer();
		// KafkaTopics.RTSERIES_SEG_GROUP
		consumer.open(Arrays.asList(KafkaTopics.RTSERIES_STATE_TOPIC), KafkaTopics.RTSERIES_SEG_GROUP, kafkaServer,
				restart);
		MetricBasedPerfProfile.registerServer(controler);
	}

	public void connectToLSMOIndex() {
		int retryCount = 0;
		while (true) {
			if (++retryCount % 20 == 0) {
				try {
					Thread.sleep(1000);

				} catch (InterruptedException e) {
				}
			}
			try {
				String[] fields = indexServer.split(":");
				String ipAddr = fields[0];
				short port = Short.parseShort(fields[1]);
				transport = new TSocket(ipAddr, port);
				TProtocol protocol = new TBinaryProtocol(transport);
				client = new TweetService.Client(protocol);
				transport.open();
				break;
			} catch (TTransportException e) {
				e.printStackTrace();
			}
		}
	}

	public void start() {
		controler.running();
		while (true) {
			susSem.acquireUninterruptibly();
			try {
				for (TimeSeriesUpdateState state : consumer.nextTSUpdateStates()) {
					onTimeSeriesUpdate(state);
				}
			} finally {
				susSem.release();
			}
		}
	}

	public void onTimeSeriesUpdate(final TimeSeriesUpdateState state) {
		// 当前时间点的转发值可能还在变，不能处理
		if (DateUtil.diffByWeiboStartTime(state.getTimestamp()) == DateUtil
				.diffByWeiboStartTime(System.currentTimeMillis()))
			return;

		List<long[]> series = null;
		final SegState segState;
		Context context = MetricBasedPerfProfile.timer("seg_query_series").time();
		try {
			SegState tmpState = segDao.getSegState(state.mid);
			segState = tmpState != null ? tmpState : new SegState(state.mid, 0);
			series = tweetDao.queryTimeSeries(state.mid, segState.lastUpdateTime, state.timestamp);
		} catch (Exception ex) {
			ex.printStackTrace();
			return;
		} finally {
			context.stop();
		}

		context = MetricBasedPerfProfile.timer("seg_index_series").time();
		try {
			SWSegmentation seg = new SWSegmentation(Long.parseLong(state.mid), 10, null, new ISegSubscriber() {
				@Override
				public void newSeg(Interval preInv, Segment seg) {
					segState.lastUpdateTime = Math.max(segState.lastUpdateTime,
							DateUtil.timeFromWeiboDate(seg.getEndTime()));
					for (int i = 0; i < 10; i++) {
						try {
							logger.info("indexing " + state + ": " + seg);
							client.indexTweetSeg(new TweetSeg(state.mid, seg.getStart(), seg.getStartCount(),
									seg.getEndTime(), seg.getEndCount()));
							break;
						} catch (TException e) {
							e.printStackTrace();
							if (e.getMessage().contains("Broken pipe")) {
								// 重新建立与索引服务器的连接
								connectToLSMOIndex();
							} else if (e.getMessage().contains("not ready")) {

							}

						}
					}
				}
			});
			for (long[] row : series) {
				seg.advance(DateUtil.diffByWeiboStartTime(row[0]), (int) row[1]);
			}
			if (state.isDegrade()) {
				seg.finish();
			}

		} finally {
			context.stop();
		}
		segDao.putSegState(segState);
	}

	/**
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		PropertyConfigurator.configure("conf/log4j.properties");
		// args = new String[] { "-c", "127.0.0.1", "-k", "localhost:9092",
		// "-l", "127.0.0.1:10000" };
		OptionParser parser = new OptionParser();
		parser.accepts("c", "cassandra server address").withRequiredArg().ofType(String.class);
		parser.accepts("k", "kafka server address").withRequiredArg().ofType(String.class);
		parser.accepts("l", "lsmo index server address").withRequiredArg().ofType(String.class);
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
		TimeSeriesSegmentation producer = new TimeSeriesSegmentation(set.valueOf("k").toString(),
				set.valueOf("c").toString(), set.valueOf("l").toString());
		producer.init(set.has("r"));
		producer.start();
		producer.stop();
	}

	private void stop() {
		if (conn != null) {
			conn.close();
			conn = null;
			consumer.close();
			transport.close();
		}
	}

	@Override
	public void finalize() {
		stop();
	}

	@Override
	public void onStop() {
		stop();
	}

	@Override
	public void onSuspend() {
		susSem.acquireUninterruptibly();
	}

	@Override
	public void onResume() {
		susSem.release();
	}
}
