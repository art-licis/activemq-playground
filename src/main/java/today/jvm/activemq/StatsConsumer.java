package today.jvm.activemq;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTopic;

import javax.jms.*;
import java.util.Enumeration;

/**
 * Stats consumer using ActiveMQ Statistics plug-in: to get stats, need to send a message
 * with a destination to get data from set to #replyTo.
 *
 * @author Arturs Licis
 */
public class StatsConsumer {
	private ConnectionFactory factory;
	private Connection connection;
	private Session session;
	private MessageConsumer consumer;

	public StatsConsumer() throws JMSException
	{
		System.out.println("[===] Starting stats consumer");
		try
		{
			ConnectionFactory factory = new ActiveMQConnectionFactory(DemoConstants.LOCALHOST_ADDR);
			connection = factory.createConnection();
			connection.start();
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);


			Queue replyTo = session.createTemporaryQueue();
			MessageConsumer consumer = session.createConsumer(replyTo);

			String queueName = "ActiveMQ.Statistics.Broker";
			Queue testQueue = session.createQueue(queueName);
			MessageProducer producer = session.createProducer(testQueue);

			boolean reached = false;
			int prevStore = -1;
			int prevMem = -1;
			while (true) {
				Message msg = session.createMessage();
				msg.setJMSReplyTo(replyTo);
				producer.send(msg);

				MapMessage reply = (MapMessage) consumer.receive();

				int storePercentUsage = reply.getInt("storePercentUsage");
				int memoryPercentUsage = reply.getInt("memoryPercentUsage");
				if (storePercentUsage != prevStore || memoryPercentUsage != prevMem) {
					System.out.printf("Current usage %% [mem/store]: %3d/%3d%n", memoryPercentUsage, storePercentUsage);
					prevStore = storePercentUsage;
					prevMem = memoryPercentUsage;
				}
				if (storePercentUsage >= 100) {
					if (!reached) {
						reached = true;
						System.out.println("[===] WARNING! Store on broker reached maximum");
					}
				}
				Thread.sleep(1000);
			}
		}
		catch (Exception e)
		{
			System.out.println("Caught:" + e);
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws JMSException {
		StatsConsumer slowConsumer = new StatsConsumer();
	}
}
