package ca.uwaterloo.swag.mavencrawler.db;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import ca.uwaterloo.swag.mavencrawler.helpers.LoggerHelper;

public class RabbitMQHandler {
	
	private String username;
	private String password;
	private String host;
	private int port;
	private String queueName;
	
	private Logger logger;
	private Connection connection;
	private Channel channel;
	
	private enum PropertyType {
		RABBITMQ_HOST,
		RABBITMQ_PORT,
		RABBITMQ_USERNAME,
		RABBITMQ_PASSWORD,
		RABBITMQ_QUEUE
	}

	// Disable default constructor
	private RabbitMQHandler() {}
	
	public static RabbitMQHandler newInstance(Logger logger) {
		RabbitMQHandler handler = new RabbitMQHandler();
		handler.logger = logger;
		
		return handler;
	}

	public static RabbitMQHandler newInstance(Logger logger, Properties properties) {
		RabbitMQHandler handler = new RabbitMQHandler();
		handler.logger = logger;
		handler.host = properties.getProperty(PropertyType.RABBITMQ_HOST.name());
		handler.port = Integer.valueOf(properties.getProperty(PropertyType.RABBITMQ_PORT.name()));
		handler.username = properties.getProperty(PropertyType.RABBITMQ_USERNAME.name());
		handler.password = properties.getProperty(PropertyType.RABBITMQ_PASSWORD.name());
		handler.queueName = properties.getProperty(PropertyType.RABBITMQ_QUEUE.name());
		
		return handler;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getQueueName() {
		return queueName;
	}

	public void setQueueName(String queueName) {
		this.queueName = queueName;
	}

	public void sendMessage(String message) {
		
		if (connect()) {
			try {
				channel.basicPublish("", queueName, null, message.getBytes());
			} catch (IOException e) {
				LoggerHelper.logError(logger, e, "Could not send message to RabbitMQ.");
			}
		}
		else {
			LoggerHelper.log(logger, Level.SEVERE, "Could not connect to RabbitMQ.");
		}
		
	}
	
	public static interface MessageHandler {
		public boolean handleMessage(String message);
	}
	
	public void listenMessages(MessageHandler messageHandler) {

		Consumer consumer = new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope,
					AMQP.BasicProperties properties, byte[] body)
							throws IOException {
				
				String message = new String(body, "UTF-8");
				System.out.println(" [x] Received '" + message + "'");
				
				if (messageHandler.handleMessage(message)) {
					this.getChannel().basicAck(envelope.getDeliveryTag(), false);
				}
				else {
					this.getChannel().basicReject(envelope.getDeliveryTag(), true);
				}
				
			}
		};

		try {
			channel.basicConsume(queueName, false, consumer);
		} catch (IOException e) {
			LoggerHelper.logError(logger, e, "Could not listen to messages from RabbitMQ.");
		}
		
	}
	
	public boolean isConnected() {
		return (channel != null && channel.isOpen());
	}

	public boolean connect() {
		
		if (isConnected()) return true;

		boolean success = false;
		
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUsername(getUsername());
		factory.setPassword(getPassword());
		factory.setHost(getHost());
		factory.setPort(getPort());
		
		try {
			connection = factory.newConnection();
			channel = connection.createChannel();
			channel.queueDeclare(queueName, true, false, false, null);
			success = channel.isOpen();
		} catch (IOException|TimeoutException e) {
			LoggerHelper.logError(logger, e, "Could not connect to RabbitMQ.");
			channel = null;
			connection = null;
		}

		return success;
	}

	public boolean disconnect() {

		boolean success = false;
		
		try {
			channel.close();
			connection.close();
			success = (!channel.isOpen() && !connection.isOpen());
		} catch (IOException | TimeoutException e) {
			LoggerHelper.logError(logger, e, "Could not disconnect to RabbitMQ.");
		} finally {
			channel = null;
			connection = null;
		}
		
		return success;
	}

}
