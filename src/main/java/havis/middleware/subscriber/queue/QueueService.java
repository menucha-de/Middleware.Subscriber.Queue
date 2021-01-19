package havis.middleware.subscriber.queue;

import havis.middleware.ale.base.exception.ImplementationException;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

public class QueueService {

	@SuppressWarnings("rawtypes")
	Map<String, Queue> queues = new ConcurrentHashMap<>();

	public void addQueue(String name, @SuppressWarnings("rawtypes") Queue queue) {
		queues.put(name, queue);
	}

	public void removeQueue(String name) {
		queues.remove(name);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public boolean sendToQueue(String name, Object report) throws ImplementationException {
		Queue queue = queues.get(name);
		if (queue != null) {
			queue.add(report);
			return true;
		}
		return false;
	}

	public boolean hasQueue(String name) {
		return queues.containsKey(name);
	}
}
