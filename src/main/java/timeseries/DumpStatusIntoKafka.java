package timeseries;

import java.io.IOException;
import java.io.PrintStream;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import io.DirLineReader;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import kafkastore.KafkaProducerFactory;
import kafkastore.KafkaTopics;
import kafkastore.TweetKafkaProducer;
import weibo4j.StatusSerDer;
import weibo4j.model.Status;

public class DumpStatusIntoKafka extends Thread {
	private final Logger logger = Logger.getLogger(DumpStatusIntoKafka.class);
	TweetKafkaProducer producer;
	DirLineReader reader;

	public DumpStatusIntoKafka() {

	}

	public void init(String kafkaServers, String dataDir, boolean restart) throws Exception {
		producer = new TweetKafkaProducer(KafkaProducerFactory.createProducer(kafkaServers));
		reader = new DirLineReader(dataDir);
	}

	@Override
	public void run() {
		logger.info("begin to consume tweets");
		String line = null;
		try {
			long count = 0;
			while (null != (line = reader.readLine())) {
				Status status = StatusSerDer.fromJSON(line);
				producer.store(KafkaTopics.RETWEET_TOPIC, status);
				if (count++ % 10000 == 0) {
					logger.info("loaded " + count + " statuses");
				}
			}
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	/**
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		PropertyConfigurator.configure("conf/log4j.properties");
		OptionParser parser = new OptionParser();
		parser.accepts("d", "data file directory").withRequiredArg().ofType(String.class);
		parser.accepts("k", "kafka server address").withRequiredArg().ofType(String.class);
		parser.accepts("r", "consume topics from the beginning");
		OptionSet set = parser.parse(args);
		if (!set.hasOptions()) {
			parser.printHelpOn(new PrintStream(System.out));
			System.exit(1);
		}

		DumpStatusIntoKafka producer = new DumpStatusIntoKafka();
		producer.init(set.valueOf("k").toString(), set.valueOf("d").toString().trim(), set.has("r"));
		producer.start();
	}
}
