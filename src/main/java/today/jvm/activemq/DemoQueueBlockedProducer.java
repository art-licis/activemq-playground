package today.jvm.activemq;

import org.apache.commons.io.FileUtils;

import java.io.File;

/**
 * Demonstrates how Producer becomes blocked upon reaching limits.
 *
 * TODO: memory % stops at around 59,- how to change that?
 * @author Arturs Licis
 */
public class DemoQueueBlockedProducer {
	public static final String QUEUE_NAME = "BL.queue.no_consumers";
	public static void main(String[] args) throws InterruptedException {
		FileUtils.deleteQuietly(new File("./activemq-data"));
		System.out.println("[===] Mirrored Queues Demo");

		runBroker();
		Thread.sleep(500);

		runProducer();
	}

	private static void runBroker() {
		Runnable r = () -> {
			try {
				EmbeddedBroker embeddedBroker = new EmbeddedBroker();
				embeddedBroker.initBroker()
						.setUsage(500_000, 1_000_000, 1_000_000)
						.enableMirroredQueues()
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
				Producer demoProducer = new Producer(QUEUE_NAME, true, 5, true);
				demoProducer.run();
			} catch (Exception e) {
				e.printStackTrace();
			}
		};
		new Thread(r).start();
	}
}
