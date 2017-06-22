package today.jvm.activemq;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * Configurable JMS Demo Producer.
 *
 * @author Arturs Licis
 */
public class Producer {
	private Session session;
	private Connection connection;
	private MessageProducer producer;
	private int messageDelay = -1;
	private boolean log = true;

	public Producer(String queueName) throws JMSException
	{
		this(queueName, true, -1, true);
	}

	public Producer(String destinationName, boolean isQueue, int messageDelay, boolean log) throws JMSException
	{
		ConnectionFactory factory = new ActiveMQConnectionFactory(DemoConstants.LOCALHOST_ADDR);
		this.connection = factory.createConnection();
		connection.start();
		this.session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Destination destination = isQueue ? session.createQueue(destinationName) : session.createTopic(destinationName);
		this.producer = session.createProducer(destination);

		if (messageDelay > 0) {
			this.messageDelay = messageDelay;
		}
		this.log = log;
	}

	public void run() throws JMSException
	{
		for (int i = 0;;i++)
		{
			if (log) {
				if (i % 100 == 0) {
					System.out.printf("%nCreating message #%d%n", i);
				} else if (i % 10 == 0) {
					System.out.print(".");
				}
			}
			Message message = session.createTextMessage("Hello World [some more data here to fill the memory]! #" + i);
			message.setIntProperty("seq", i);
			producer.send(message);
			if (messageDelay > 0) {
				try {
					Thread.sleep(messageDelay);
				} catch (InterruptedException e) {
				}
			}
		}
	}

	public void close() throws JMSException
	{
		if (connection != null)
		{
			connection.close();
		}
	}

	public static void main(String[] args) throws JMSException {
		Producer fp = new Producer("BL.test", true, 100, true);
		fp.run();
		fp.close();
	}
}
