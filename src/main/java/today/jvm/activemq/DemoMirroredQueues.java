package today.jvm.activemq;

import org.apache.commons.io.FileUtils;

import java.io.File;

/**
 * Demonstration of round robin policy issues - slow consumer will stay much behind,
 * and messages will be left unconsumed.
 *
 * @author Arturs Licis
 */
public class DemoMirroredQueues {
	public static final String QUEUE_NAME = "BL.queue.mirrored";

	public static void main(String[] args) throws InterruptedException {
		new DemoMirroredQueues().runDemo();
	}

	public DemoMirroredQueues() {
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
		Thread.sleep(1000);

		System.out.println("[===] Starting Consumers (fast & slow)");
		runConsumer("[FAST]", -1, QUEUE_NAME, true);
		runConsumer("[SLOW]", 4000, QUEUE_NAME,true);
		runConsumer("[MIRROR]", -1, "_mirrored_." + QUEUE_NAME,false);
	}

	private void runBroker() {
		Runnable r = () -> {
			try {
				EmbeddedBroker embeddedBroker = new EmbeddedBroker();
				embeddedBroker.initBroker()
						.setUsage(2_000_000, 4_000_000, 4_000_000)
						.enableMirroredQueues()
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
				Producer demoProducer = new Producer(QUEUE_NAME, true, 1000, false);
				demoProducer.run();
			} catch (Exception e) {
				e.printStackTrace();
			}
		};
		new Thread(r).start();
	}

	private void runConsumer(final String name, final int delay, String destination, boolean isQueue) {
		Runnable r = () -> {
			try {
				Consumer demoConsumer = new Consumer(name, destination, delay, getUriParams(), isQueue);
				demoConsumer.run();
			} catch (Exception e) {
				e.printStackTrace();
			}
		};
		new Thread(r).start();
	}
}
