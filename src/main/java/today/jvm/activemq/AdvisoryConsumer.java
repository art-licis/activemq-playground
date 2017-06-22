package today.jvm.activemq;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;

import javax.jms.*;

/**
 * Demo Advisory Consumer.
 *
 * @author Arturs Licis
 */
public class AdvisoryConsumer implements MessageListener {
	private String consumerName;
	private final String destinationName;
	private final boolean isQueue;

	@Override
	public void onMessage(Message message) {
		try {
			if (message instanceof ActiveMQMessage) {
				System.out.println(consumerName + " :: " + ((ActiveMQMessage) message).getDestination() + " : Message received: " + message);
			} else {
				System.out.println("Invalid message received.");
			}
		} catch (Exception e) {
			System.out.println("Caught:" + e);
			e.printStackTrace();
		}
	}

	public AdvisoryConsumer(String consumerName, String destinationName, boolean isQueue) throws JMSException {
		System.out.println("[===] Starting advisory consumer");
		this.consumerName = consumerName;
		this.destinationName = destinationName;
		this.isQueue = isQueue;
	}

	public void run() throws JMSException {
		try {
			ConnectionFactory factory = new ActiveMQConnectionFactory(DemoConstants.LOCALHOST_ADDR);
			Connection connection = factory.createConnection();
			connection.start();
			Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Destination destination = isQueue ? session.createQueue(destinationName) : session.createTopic(destinationName);

			final Topic advisoryFastProducerTopic = AdvisorySupport.getFastProducerAdvisoryTopic((ActiveMQDestination) destination);
			final Topic advisorySlowConsumerTopic = AdvisorySupport.getFastProducerAdvisoryTopic((ActiveMQDestination) destination);
			final Topic advisoryWhenFullTopic = AdvisorySupport.getFullAdvisoryTopic((ActiveMQDestination) destination);

			MessageConsumer consumerFastProducer = session.createConsumer(advisoryFastProducerTopic);
			MessageConsumer consumerWhenFull = session.createConsumer(advisoryWhenFullTopic);
			consumerFastProducer.setMessageListener(this);
			consumerWhenFull.setMessageListener(this);
		} catch (Exception e) {
			System.out.println("Caught:" + e);
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws JMSException {
		AdvisoryConsumer advisoryConsumer = new AdvisoryConsumer("Demo","BL.test", true);
	}
}
