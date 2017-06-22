package today.jvm.activemq;

import org.apache.commons.io.FileUtils;

import java.io.File;

/**
 * Demonstration of round robin policy issues - slow consumer will stay much behind,
 * and messages will be left unconsumed.
 *
 * @author Arturs Licis
 */
public class DemoRoundRobinNoPrefetch extends DemoRoundRobin {
	@Override
	protected String getUriParams() {
		return "?jms.prefetchPolicy.queuePrefetch=1";
	}

	public static void main(String[] args) throws InterruptedException {
		new DemoRoundRobin().runDemo();
	}
}
