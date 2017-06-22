package today.jvm.activemq;


/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import javax.jms.*;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.virtual.MirroredQueue;
import org.apache.activemq.command.ActiveMQDestination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class AMQ3903Test {

	private static final transient Logger LOG = LoggerFactory.getLogger(AMQ3903Test.class);

	private static final String bindAddress = "tcp://0.0.0.0:0";
	private BrokerService broker;
	private ActiveMQConnectionFactory cf;

	private static final int MESSAGE_COUNT = 100;

	public static void main(String[] args) throws Exception {
		new AMQ3903Test();
	}

	public AMQ3903Test() throws Exception {
		setUp();

		testAdvisoryForFastGenericProducer();
		testAdvisoryForFastDedicatedProducer();

		Thread.sleep(100000);

		tearDown();
	}

	public void setUp() throws Exception {
		broker = this.createBroker();
		String address = broker.getTransportConnectors().get(0).getPublishableConnectString();
		broker.start();
		broker.waitUntilStarted();

		cf = new ActiveMQConnectionFactory(address);
	}

	public void tearDown() throws Exception {
		if (broker != null) {
			broker.stop();
			broker.waitUntilStopped();
		}
	}

	public void testAdvisoryForFastGenericProducer() throws Exception {
		doTestAdvisoryForFastProducer(true);
	}

	public void testAdvisoryForFastDedicatedProducer() throws Exception {
		doTestAdvisoryForFastProducer(false);
	}

	public void doTestAdvisoryForFastProducer(boolean genericProducer) throws Exception {

		Connection connection = cf.createConnection();
		connection.start();
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

		final Queue queue = session.createQueue("Blah");

		final Topic advisoryTopic = AdvisorySupport.getFastProducerAdvisoryTopic((ActiveMQDestination) queue);
		final Topic advisoryWhenFullTopic = AdvisorySupport.getFullAdvisoryTopic((ActiveMQDestination) queue);

		MessageConsumer advisoryConsumer = session.createConsumer(advisoryTopic);
		MessageConsumer advisoryWhenFullConsumer = session.createConsumer(advisoryWhenFullTopic);

		MessageProducer producer = session.createProducer(genericProducer ? null : queue);

		try {
			// send lots of messages to the tempQueue
			for (int i = 0; i < MESSAGE_COUNT; i++) {
				BytesMessage m = session.createBytesMessage();
				m.writeBytes(new byte[1024]);
				if (genericProducer) {
					producer.send(queue, m, DeliveryMode.PERSISTENT, 4, 0);
				} else {
					producer.send(m);
				}
			}
		} catch (ResourceAllocationException expectedOnLimitReachedAfterFastAdvisory) {}

		// check one advisory message has produced on the advisoryTopic
		Message advCmsg = advisoryConsumer.receive(4000);
		System.out.println("Got 'FAST PRODUCER' advisory message: " + advCmsg);

		advCmsg = advisoryWhenFullConsumer.receive(4000);
		System.out.println("Got 'FULL' advisory message: " + advCmsg);

		connection.close();
		LOG.debug("Connection closed, destinations should now become inactive.");
	}

	protected BrokerService createBroker() throws Exception {
		BrokerService answer = new BrokerService();
		answer.setPersistent(false);
		answer.setUseJmx(false);

		PolicyEntry entry = new PolicyEntry();
		entry.setAdvisoryForFastProducers(true);
		entry.setAdvisoryWhenFull(true);
		entry.setMemoryLimit(10000);
		PolicyMap map = new PolicyMap();
		map.setDefaultEntry(entry);

		answer.setDestinationPolicy(map);
		answer.addConnector(bindAddress);

		answer.getSystemUsage().setSendFailIfNoSpace(true);

		return answer;
	}
}
