package today.jvm.activemq;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * Configurable JMS Demo Consumer.
 *
 * @author Arturs Licis
 */
public class Consumer implements MessageListener {
	private String consumerName;
	private final String destinationName;
	private final String uriParams;
	private int processDelay = -1;
	private final boolean isQueue;

	@Override
	public void onMessage(Message message) {
		try {
			if (message instanceof TextMessage) {
				TextMessage txtMessage = (TextMessage) message;
				int seq = message.getIntProperty(DemoConstants.SEQ_PROPERTY);
				System.out.println(consumerName + " : Message received: #" + seq);
				if (processDelay > 0) {
					try {
						Thread.sleep(processDelay);
					} catch (InterruptedException e) {
					}
				}
			} else {
				System.out.println("Invalid message received.");
			}
		} catch (JMSException e) {
			System.out.println("Caught:" + e);
			e.printStackTrace();
		}
	}

	public Consumer(String consumerName, String destinationName, int processDelay, String uriParams, boolean isQueue) throws JMSException {
		this.consumerName = consumerName;
		this.destinationName = destinationName;
		this.processDelay = processDelay;
		this.uriParams = uriParams;
		this.isQueue = isQueue;
	}

	public void run() throws JMSException {
		try {
			ConnectionFactory factory = new ActiveMQConnectionFactory(DemoConstants.LOCALHOST_ADDR + uriParams);
			Connection connection = factory.createConnection();
			connection.start();
			Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Destination destination = isQueue ? session.createQueue(destinationName) : session.createTopic(destinationName);
			MessageConsumer consumer = session.createConsumer(destination);
			consumer.setMessageListener(this);
		} catch (Exception e) {
			System.out.println("Caught:" + e);
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws JMSException {
		Consumer slowConsumer = new Consumer("Demo","BL.test", -1, "", true);
	}
}
