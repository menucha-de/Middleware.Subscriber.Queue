package havis.middleware.subscriber.queue;

import havis.middleware.ale.base.exception.ImplementationException;
import havis.middleware.ale.base.exception.InvalidURIException;
import havis.middleware.ale.exit.Exits;
import havis.middleware.ale.service.cc.CCReports;
import havis.middleware.ale.service.ec.ECReports;
import havis.middleware.ale.service.pc.PCReports;
import havis.middleware.ale.subscriber.SubscriberConnector;

import java.net.URI;
import java.util.Map;

public class QueueSubscriberConnector implements SubscriberConnector {

	public static QueueService QUEUE_SERVICE;

	private String name;

	private boolean inErrorState = false;

	private URI uri;

	@Override
	public void init(URI uri, Map<String, String> properties) throws InvalidURIException, ImplementationException {
		this.uri = uri;
		if (QUEUE_SERVICE != null) {
			switch (uri.getScheme()) {
			case "queue":
				this.name = uri.getAuthority();
				break;
			default:
				throw new InvalidURIException("Unknown scheme '" + uri.getScheme() + "'");
			}
		} else {
			throw new ImplementationException("Could not get any queue");
		}
	}

	private void sendReport(Object report) throws ImplementationException {
		if (QUEUE_SERVICE != null) {
			if (QUEUE_SERVICE.sendToQueue(this.name, report)) {
				if (inErrorState) {
					inErrorState = false;
					Exits.Log.logp(Exits.Level.Information, Exits.Subscriber.Controller.Name, Exits.Subscriber.Container.DeliverFailed,
							"Deliver to subscriber {0} succeeded: {1}", new Object[] { this.uri.toString(), "Queue is available again.", report });
				}
			} else if (!inErrorState) {
				inErrorState = true;
				Exits.Log.logp(Exits.Level.Error, Exits.Subscriber.Controller.Name, Exits.Subscriber.Controller.DeliverFailed,
						"Deliver to subscriber {0} failed: {1}", new Object[] { this.uri.toString(), "Queue is currently not available, reports will be lost!",
								report });
			}
		} else {
			throw new ImplementationException("Could not get any queue");
		}
	}

	@Override
	public void send(ECReports reports) throws ImplementationException {
		sendReport(reports);
	}

	@Override
	public void send(CCReports reports) throws ImplementationException {
		sendReport(reports);
	}

	@Override
	public void send(PCReports reports) throws ImplementationException {
		sendReport(reports);
	}

	@Override
	public void dispose() throws ImplementationException {
	}
}