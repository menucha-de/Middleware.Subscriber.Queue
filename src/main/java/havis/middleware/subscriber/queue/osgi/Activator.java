package havis.middleware.subscriber.queue.osgi;

import havis.middleware.ale.subscriber.SubscriberConnector;
import havis.middleware.subscriber.queue.QueueService;
import havis.middleware.subscriber.queue.QueueSubscriberConnector;

import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.osgi.framework.Bundle;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.PrototypeServiceFactory;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;
import org.osgi.util.tracker.ServiceTracker;

public class Activator implements BundleActivator {

	private final Logger log = Logger.getLogger(Activator.class.getName());

	private final static String NAME = "name";
	private final static String VALUE = "queue";
	
	@SuppressWarnings("rawtypes")
	private ServiceTracker<Queue, Queue> tracker;

	private ServiceRegistration<?> registration;

	@SuppressWarnings("rawtypes")
	@Override
	public void start(final BundleContext context) throws Exception {
		QueueSubscriberConnector.QUEUE_SERVICE = new QueueService();
		tracker = new ServiceTracker<Queue, Queue>(context, Queue.class, null) {
			@Override
			public Queue addingService(ServiceReference<Queue> reference) {
				Queue service = super.addingService(reference);
				Object name = reference.getProperty(NAME);
				if (name instanceof String) {
					log.log(Level.FINE, "Adding queue {0}.", (String) name);
					QueueSubscriberConnector.QUEUE_SERVICE.addQueue((String) name, service);
				}
				return service;
			}

			@Override
			public void removedService(ServiceReference<Queue> reference, Queue service) {
				Object name = reference.getProperty(NAME);
				if (name instanceof String) {
					log.log(Level.FINE, "Removing queue {0}.", (String) name);
					QueueSubscriberConnector.QUEUE_SERVICE.removeQueue((String) name);
				}
				super.removedService(reference, service);
			}
		};
		tracker.open();

		Dictionary<String, String> properties = new Hashtable<>();
		properties.put(NAME, VALUE);

		log.log(Level.FINE, "Register prototype service factory {0} ({1}={2})", new Object[] { QueueSubscriberConnector.class.getName(), NAME, VALUE });
		registration = context.registerService(SubscriberConnector.class.getName(), new PrototypeServiceFactory<SubscriberConnector>() {
			@Override
			public SubscriberConnector getService(Bundle bundle, ServiceRegistration<SubscriberConnector> registration) {
				return new QueueSubscriberConnector();
			}

			@Override
			public void ungetService(Bundle bundle, ServiceRegistration<SubscriberConnector> registration, SubscriberConnector service) {
			}
		}, properties);
	}

	@Override
	public void stop(BundleContext context) throws Exception {
		if (registration != null) {
			registration.unregister();
		}
		if (tracker != null) {
			tracker.close();
			QueueSubscriberConnector.QUEUE_SERVICE = null;
		}
	}
}