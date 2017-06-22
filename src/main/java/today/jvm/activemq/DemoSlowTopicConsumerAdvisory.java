package today.jvm.activemq;

import org.apache.commons.io.FileUtils;

import java.io.File;

/**
 * Demo scenario of Advisory messages being sent for slow topic consumer.
 *
 * WORK IN PROGRESS
 *
 * @author Arturs Licis
 */
public class DemoSlowTopicConsumerAdvisory {
	public static final String TOPIC_NAME = "BL.topic.slow_consumer";
	public static void main(String[] args) throws InterruptedException {
		FileUtils.deleteQuietly(new File("./activemq-data"));
		System.out.println("[===] Mirrored Queues Demo");

		runBroker();
		Thread.sleep(500);
		runProducer();
		runConsumer();
		runAdvisoryConsumer();
	}

	private static void runBroker() {
		Runnable r = () -> {
			try {
				EmbeddedBroker embeddedBroker = new EmbeddedBroker();
				embeddedBroker.initBroker()
						.setUsage(5_000_000, 20_000_000, 20_000_000)
						.start();
			} catch (Exception e) {
				e.printStackTrace();
			}
		};

		new Thread(r).start();
	}

	private static void runProducer() {
		Runnable r = () -> {
			try {
				Producer demoProducer = new Producer(TOPIC_NAME, true, -1, true);
				demoProducer.run();
			} catch (Exception e) {
				e.printStackTrace();
			}
		};
		new Thread(r).start();
	}

	private static void runConsumer() {
		Runnable r = () -> {
			try {
				Consumer demoConsumer = new Consumer("[SLOW]", TOPIC_NAME, 100, "", false);
				demoConsumer.run();
			} catch (Exception e) {
				e.printStackTrace();
			}
		};
		new Thread(r).start();
	}

	private static void runAdvisoryConsumer() {
		Runnable r = () -> {
			try {
				AdvisoryConsumer advisoryConsumer = new AdvisoryConsumer("[ADV]", TOPIC_NAME, false);
				advisoryConsumer.run();
			} catch (Exception e) {
				e.printStackTrace();
			}
		};
		new Thread(r).start();
	}
}
