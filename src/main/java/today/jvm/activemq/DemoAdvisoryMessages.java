package today.jvm.activemq;

import org.apache.commons.io.FileUtils;

import java.io.File;

/**
 * Demo scenario of Advisory messages being sent upon fast producer/memory limits exceeding.
 *
 * WORK IN PROGRESS
 *
 * TODO: still can't make this work when Broker has persistence switched on.
 *
 * @author Arturs Licis
 */
public class DemoAdvisoryMessages {
	public static final String QUEUE_NAME = "BL.queue.no_consumers";
	public static void main(String[] args) throws InterruptedException {
		FileUtils.deleteQuietly(new File("./activemq-data"));
		System.out.println("[===] Mirrored Queues Demo");

		runBroker();
		Thread.sleep(500);
		runProducer();
		runAdvisoryConsumer();
	}

	private static void runBroker() {
		Runnable r = () -> {
			try {
				EmbeddedBroker embeddedBroker = new EmbeddedBroker();
				embeddedBroker.initBroker()
						.setUsage(500_000, 1_000_000, 1_000_000)
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
				Producer demoProducer = new Producer(QUEUE_NAME, true, -1, true);
				demoProducer.run();
			} catch (Exception e) {
				e.printStackTrace();
			}
		};
		new Thread(r).start();
	}

	private static void runAdvisoryConsumer() {
		Runnable r = () -> {
			try {
				AdvisoryConsumer advisoryConsumer = new AdvisoryConsumer("[ADV]", QUEUE_NAME, true);
				advisoryConsumer.run();
			} catch (Exception e) {
				e.printStackTrace();
			}
		};
		new Thread(r).start();
	}
}
