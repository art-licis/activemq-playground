package today.jvm.activemq;

import org.apache.commons.io.FileUtils;

import java.io.File;

/**
 * Demonstration of how statistics plugin can be used to monitor memory/store consumption.
 *
 * @author Arturs Licis
 */
public class DemoStatsConsumer {
	public static final String QUEUE_NAME = "BL.queue.no_consumers";
	public static void main(String[] args) throws InterruptedException {
		FileUtils.deleteQuietly(new File("./activemq-data"));
		System.out.println("[===] Mirrored Queues Demo");

		runBroker();
		Thread.sleep(500);

		runStatsConsumer();
		Thread.sleep(1000);

		runProducer();
	}

	private static void runBroker() {
		Runnable r = () -> {
			try {
				EmbeddedBroker embeddedBroker = new EmbeddedBroker();
				embeddedBroker.initBroker()
						.setUsage(2_000_000, 4_000_000, 4_000_000)
						.enableStatisticsPlugin()
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
				Producer demoProducer = new Producer(QUEUE_NAME, true, 5, false);
				demoProducer.run();
			} catch (Exception e) {
				e.printStackTrace();
			}
		};
		new Thread(r).start();
	}

	private static void runStatsConsumer() {
		Runnable r = () -> {
			try {
				new StatsConsumer();
			} catch (Exception e) {
				e.printStackTrace();
			}
		};
		new Thread(r).start();
	}
}
