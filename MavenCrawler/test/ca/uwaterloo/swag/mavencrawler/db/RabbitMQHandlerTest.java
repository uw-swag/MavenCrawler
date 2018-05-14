package ca.uwaterloo.swag.mavencrawler.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Properties;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ca.uwaterloo.swag.mavencrawler.db.RabbitMQHandler.MessageHandler;

public class RabbitMQHandlerTest {
	
	private RabbitMQHandler handler;
	private boolean messageConfirmation;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	/**
	 * TODO: use mock instead of actual server
	 */
	@Before
	public void setUp() throws Exception {
		handler = RabbitMQHandler.newInstance(Logger.getLogger(RabbitMQHandlerTest.class.getName()));
		handler.setHost("localhost");
		handler.setPort(5672);
		handler.setUsername("guest");
		handler.setPassword("guest");
		handler.setQueueName("test_queue");
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@Test
	public void testFactoryMethodWithProperties() throws IOException {
		
		// Given
		Logger aLogger = Logger.getLogger(this.getClass().getName());
		Properties properties = new Properties();
		properties.load(this.getClass().getResourceAsStream("../crawler-config-example.conf"));
		
		// When
		RabbitMQHandler handler = RabbitMQHandler.newInstance(aLogger, properties);
		
		// Then
		assertNotNull(handler);
		assertEquals("127.0.0.1", handler.getHost());
		assertEquals(10000, handler.getPort());
		assertEquals("rabbitmq_username", handler.getUsername());
		assertEquals("rabbitmq_password", handler.getPassword());
		assertEquals("queue", handler.getQueueName());
	}
	
	/**
	 * Uncomment @Test to run it on an actual server.
	 */
//	@Test
	public void testConnect() {

		// Given
		// initial parameters
		
		// When
		assertTrue(handler.connect());
		
		// Then
		assertTrue(handler.isConnected());
	}

	/**
	 * Uncomment @Test to run it on an actual server.
	 */
//	@Test
	public void testDisconnect() {

		// Given
		assertTrue(handler.connect());
		
		// When
		assertTrue(handler.disconnect());
		
		// Then
		assertFalse(handler.isConnected());
	}

	/**
	 * Uncomment @Test to run it on an actual server.
	 */
//	@Test
	public void testMessaging() throws InterruptedException {
		
		// Given
		String sentMessage = "testMessage";
		messageConfirmation = false;
		
		MessageHandler messageHandler = new MessageHandler() {
			@Override
			public boolean handleMessage(String receivedMessage) {
				assertEquals(sentMessage, receivedMessage);
				messageConfirmation = receivedMessage.equals(sentMessage);
				return messageConfirmation;
			}; 
		};
		
		// When
		handler.sendMessage(sentMessage);
		handler.listenMessages(messageHandler);
		Thread.sleep(200);
		
		// Then
		assertTrue(messageConfirmation);
	}

	/**
	 * Uncomment @Test to run it on an actual server.
	 */
//	@Test
	public void testListenFirstShouldConnect() throws InterruptedException {
		
		// Given
		String sentMessage = "testMessage";
		messageConfirmation = false;
		
		MessageHandler messageHandler = new MessageHandler() {
			@Override
			public boolean handleMessage(String receivedMessage) {
				assertEquals(sentMessage, receivedMessage);
				messageConfirmation = receivedMessage.equals(sentMessage);
				return messageConfirmation;
			}; 
		};
		
		// When
		handler.listenMessages(messageHandler);
		handler.sendMessage(sentMessage);
		Thread.sleep(200);
		
		// Then
		assertTrue(messageConfirmation);
	}

}
