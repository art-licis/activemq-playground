package today.jvm.activemq;

import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.virtual.MirroredQueue;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.plugin.StatisticsBrokerPlugin;
import org.apache.activemq.usage.MemoryUsage;
import org.apache.activemq.usage.StoreUsage;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.usage.TempUsage;

/**
 * Embedded {@link BrokerService} factory.
 *
 * @author Arturs Licis
 */
public class EmbeddedBroker {
	private BrokerService broker;

	public EmbeddedBroker initBroker() {
		System.out.println("[===] Creating Broker...");
		this.broker = new BrokerService();

		//broker.setPersistent(false);

		PolicyEntry policyEntry = new PolicyEntry();
		policyEntry.setAdvisoryForFastProducers(true);
		policyEntry.setAdvisoryForSlowConsumers(true);
		policyEntry.setAdvisoryWhenFull(true);
		policyEntry.setMemoryLimit(3_000_000);

		PolicyMap policyMap = new PolicyMap();
		policyMap.setDefaultEntry(policyEntry);

		broker.setDestinationPolicy(policyMap);

		broker.getSystemUsage().setSendFailIfNoSpace(true);

		broker.setBrokerName("master");
		broker.setEnableStatistics(true);
		broker.setUseJmx(true);

		broker.setPersistent(false);

		return this;
	}

	public EmbeddedBroker setPersistent(boolean persistent) {
		broker.setPersistent(persistent);

		return this;
	}

	public EmbeddedBroker setUsage(int memoryUsageBytes, int storeUsageBytes, int tempUsageBytes) {
		MemoryUsage memoryUsage = new MemoryUsage();
		memoryUsage.setLimit(memoryUsageBytes);
		StoreUsage storeUsage = new StoreUsage();
		storeUsage.setLimit(storeUsageBytes);
		TempUsage tempUsage = new TempUsage();
		tempUsage.setLimit(tempUsageBytes);

		SystemUsage usage = broker.getSystemUsage();
		usage.setMemoryUsage(memoryUsage);
		usage.setStoreUsage(storeUsage);
		usage.setTempUsage(tempUsage);

		broker.setSystemUsage(usage);

		return this;
	}

	public EmbeddedBroker setSplitSystemUsage(int producerPortion, int consumerPortion) {
		broker.setSplitSystemUsageForProducersConsumers(true);
		broker.setProducerSystemUsagePortion(producerPortion);
		broker.setConsumerSystemUsagePortion(consumerPortion);

		return this;
	}

	public EmbeddedBroker enableStatisticsPlugin() {
		broker.setPlugins(new BrokerPlugin[] { new StatisticsBrokerPlugin() } );

		return this;
	}

	public EmbeddedBroker enableMirroredQueues() {
		return this.enableMirroredQueues("_mirrored_.", "");
	}

	public EmbeddedBroker enableMirroredQueues(String prefix, String postfix) {
		broker.setUseMirroredQueues(true);
		MirroredQueue mQueueInterceptor = new MirroredQueue();
		mQueueInterceptor.setPrefix(prefix);
		mQueueInterceptor.setPostfix(postfix);
		DestinationInterceptor[] interceptors = new DestinationInterceptor[] { mQueueInterceptor };
		broker.setDestinationInterceptors(interceptors);

		return this;
	}

	public void start() throws Exception {
		broker.addConnector(DemoConstants.LOCALHOST_ADDR);
		broker.start();
		System.out.println("[===] Started Broker...");
	}

	public BrokerService getBroker() {
		return broker;
	}
	public static void main(String[] args) throws Exception {
		EmbeddedBroker embeddedBroker = new EmbeddedBroker();
		embeddedBroker.initBroker();
		embeddedBroker.setUsage(3_000_000, 5_000_000, 5_000_000);
		embeddedBroker.start();
	}
}
