package org.apache.flink.streaming.controlplane.rescale;

import org.apache.flink.runtime.controlplane.streammanager.StreamManagerGateway;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rescale.JobRescaleAction;
import org.apache.flink.runtime.rescale.JobRescalePartitionAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Queue;


public class RescaleActionConsumer implements Runnable {

	private static final Logger LOG = LoggerFactory.getLogger(RescaleActionConsumer.class);

	private final StreamManagerGateway localStreamManagerGateway;

	private final Queue<JobRescaleAction.RescaleParamsWrapper> queue;

	private boolean isFinished;

	private volatile boolean isStop;

	public RescaleActionConsumer(StreamManagerGateway localStreamManagerGateway) {
		this.localStreamManagerGateway = localStreamManagerGateway;
		this.queue = new LinkedList<>();
	}

	@Override
	public void run() {
		while (!isStop) {
			synchronized (queue) {
				try {
					while (queue.isEmpty() && !isStop) {
						queue.wait(); // wait for new input
					}
					if (isStop) {
						return;
					}
					JobRescaleAction.RescaleParamsWrapper wrapper = queue.poll();
					if (wrapper != null) {
						isFinished = false;
//						rescaleAction.parseParams(wrapper);
						localStreamManagerGateway.rescaleStreamJob(wrapper);

						while (!isFinished && !isStop) {
							queue.wait(); // wait for finish
						}
						Thread.sleep(1000); // 30ms delay for fully deployment
					}
				} catch (Exception e) {
					e.printStackTrace();
					LOG.error("++++++ RescaleActionQueue err: ", e);
					return;
				}
			}
		}
	}

	public void put(
		JobRescaleAction.ActionType type,
		JobVertexID vertexID,
		int newParallelism,
		JobRescalePartitionAssignment jobRescalePartitionAssignment) {

		put(new JobRescaleAction.RescaleParamsWrapper(type, vertexID, newParallelism, jobRescalePartitionAssignment));
	}

	public void put(JobRescaleAction.RescaleParamsWrapper wrapper) {
		synchronized (queue) {
			queue.offer(wrapper);
			queue.notify();
		}
	}

	public void notifyFinished() {
		synchronized (queue) {
			isFinished = true;
			queue.notify();
		}
	}

	public void stopGracefully() {
		synchronized (queue) {
			isStop = true;
			queue.notify();
		}
	}
}


