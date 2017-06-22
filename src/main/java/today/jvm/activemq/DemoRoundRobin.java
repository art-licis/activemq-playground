package today.jvm.activemq;

import org.apache.commons.io.FileUtils;

import java.io.File;

/**
 * Demonstration of round robin policy issues - slow consumer will stay much behind,
 * and messages will be left unconsumed.
 *
 * @author Arturs Licis
 */
public class DemoRoundRobin {
	public static final String QUEUE_NAME = "BL.queue.round_robin";

	public static void main(String[] args) throws InterruptedException {
		new DemoRoundRobin().runDemo();
	}

	public DemoRoundRobin() {
	}

	protected String getUriParams() {
		return "";
	}

	public void runDemo() throws InterruptedException {
		FileUtils.deleteQuietly(new File("./activemq-data"));
		System.out.println("[===] Round Robin Dispatch Policy Demo");

		runBroker();
		Thread.sleep(500);

		System.out.println("[===] Starting Producer");
		runProducer();
		Thread.sleep(500);

		System.out.println("[===] Starting Consumers (fast & slow)");
		runConsumer("[FAST]", -1);
		runConsumer("[SLOW]", 2000);
	}

	private void runBroker() {
		Runnable r = () -> {
			try {
				EmbeddedBroker embeddedBroker = new EmbeddedBroker();
				embeddedBroker.initBroker()
						.setUsage(2_000_000, 4_000_000, 4_000_000)
						.start();
			} catch (Exception e) {
				e.printStackTrace();
			}
		};

		new Thread(r).start();
	}

	private void runProducer() {
		Runnable r = () -> {
			try {
				Producer demoProducer = new Producer(QUEUE_NAME, true, 100, false);
				demoProducer.run();
			} catch (Exception e) {
				e.printStackTrace();
			}
		};
		new Thread(r).start();
	}

	private void runConsumer(final String name, final int delay) {
		Runnable r = () -> {
			try {
				Consumer demoConsumer = new Consumer(name, QUEUE_NAME, delay, getUriParams(), true);
				demoConsumer.run();
			} catch (Exception e) {
				e.printStackTrace();
			}
		};
		new Thread(r).start();
	}
}
