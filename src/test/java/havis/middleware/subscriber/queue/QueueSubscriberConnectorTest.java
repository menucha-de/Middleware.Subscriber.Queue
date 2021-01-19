package havis.middleware.subscriber.queue;

import havis.middleware.ale.base.exception.ImplementationException;
import havis.middleware.ale.base.exception.InvalidURIException;
import havis.middleware.ale.service.cc.CCReports;
import havis.middleware.ale.service.ec.ECReports;
import havis.middleware.ale.service.pc.PCReports;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.Assert;
import org.junit.Test;

public class QueueSubscriberConnectorTest {

	@Test
	public void subscriberTest() throws URISyntaxException, ImplementationException, InvalidURIException {

		QueueSubscriberConnector connector = new QueueSubscriberConnector();
		try {
			connector.init(null, new HashMap<String, String>());
			Assert.fail();
		} catch (ImplementationException e) {
			Assert.assertEquals(e.getReason(), "Could not get any queue");
		}

		QueueSubscriberConnector.QUEUE_SERVICE = new QueueService();

		try {
			connector.init(new URI("unknown:test"), new HashMap<String, String>());
		} catch (InvalidURIException e) {
			Assert.assertEquals(e.getReason(), "Unknown scheme 'unknown'");
		}

		try {
			connector.init(new URI("queue://test"), new HashMap<String, String>());
		} catch (InvalidURIException e) {
			Assert.assertEquals(e.getReason(), "Unknown queue 'test'");
		}

		@SuppressWarnings("rawtypes")
		Queue queue = new LinkedBlockingQueue<>();
		QueueSubscriberConnector.QUEUE_SERVICE.addQueue("test", queue);

		connector.init(new URI("queue://test"), new HashMap<String, String>());

		ECReports ecReports = new ECReports();
		connector.send(ecReports);
		Assert.assertEquals(ecReports, queue.poll());

		CCReports ccReports = new CCReports();
		connector.send(ccReports);
		Assert.assertEquals(ccReports, queue.poll());

		PCReports pcReports = new PCReports();
		connector.send(pcReports);
		Assert.assertEquals(pcReports, queue.poll());

		connector.dispose();
	}
}