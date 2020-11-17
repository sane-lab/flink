package org.apache.flink.runtime.rescale.reconfigure;

import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.controlplane.PrimitiveOperation;
import org.apache.flink.runtime.controlplane.StreamRelatedInstanceFactory;
import org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptor;
import org.apache.flink.runtime.controlplane.abstraction.StreamJobExecutionPlan;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;


public abstract class AbstractCoordinator implements PrimitiveOperation {

	protected JobGraph jobGraph;
	protected ExecutionGraph executionGraph;
	protected ClassLoader userCodeClassLoader;

	private StreamRelatedInstanceFactory streamRelatedInstanceFactory;

	private JobGraphUpdater updater;
	private StreamJobExecutionPlan heldExecutionPlan;
	protected Map<Integer, OperatorID> operatorIDMap;


	protected AbstractCoordinator(JobGraph jobGraph, ExecutionGraph executionGraph) {
		this.jobGraph = jobGraph;
		this.executionGraph = executionGraph;
		this.userCodeClassLoader = executionGraph.getUserClassLoader();
	}

	public void setStreamRelatedInstanceFactory(StreamRelatedInstanceFactory streamRelatedInstanceFactory) {
		heldExecutionPlan = streamRelatedInstanceFactory.createExecutionPlan(jobGraph, executionGraph, userCodeClassLoader);
		updater = streamRelatedInstanceFactory.createJobGraphUpdater(jobGraph, userCodeClassLoader);
		operatorIDMap = updater.getOperatorIDMap();
		this.streamRelatedInstanceFactory = streamRelatedInstanceFactory;
	}

	public StreamJobExecutionPlan getHeldExecutionPlanCopy(){
		StreamJobExecutionPlan executionPlan = streamRelatedInstanceFactory.createExecutionPlan(jobGraph, executionGraph, userCodeClassLoader);
		for (Iterator<OperatorDescriptor> it = executionPlan.getAllOperatorDescriptor(); it.hasNext(); ) {
			OperatorDescriptor descriptor = it.next();
			OperatorDescriptor heldDescriptor = heldExecutionPlan.getOperatorDescriptorByID(descriptor.getOperatorID());
			// make sure udf share the same reference so we could identity the change if any
			descriptor.setUdf(heldDescriptor.getUdf());
		}
		return executionPlan;
	}

	protected JobVertexID rawVertexIDToJobVertexID(int rawID){
		OperatorID operatorID = operatorIDMap.get(rawID);
		if(operatorID == null){
			return null;
		}
		for(JobVertex vertex: jobGraph.getVertices()){
			if(vertex.getOperatorIDs().contains(operatorID)){
				return vertex.getID();
			}
		}
		return null;
	}

	@Override
	public CompletableFuture<Void> prepareExecutionPlan(StreamJobExecutionPlan jobExecutionPlan) {
		for (Iterator<OperatorDescriptor> it = jobExecutionPlan.getAllOperatorDescriptor(); it.hasNext(); ) {
			OperatorDescriptor descriptor = it.next();
			int operatorID = descriptor.getOperatorID();
			OperatorDescriptor heldDescriptor = heldExecutionPlan.getOperatorDescriptorByID(operatorID);
			ChangedPosition changedPosition = analyzeOperatorDifference(heldDescriptor, descriptor);
			try {
				switch (changedPosition) {
					case UDF:
						heldDescriptor.setUdf(descriptor.getUdf());
						updater.updateOperator(operatorID, descriptor.getUdf());
						break;
					case NO_CHANGE:
				}
			}catch (Exception e){
				e.printStackTrace();
				return FutureUtils.completedExceptionally(e);
			}
		}
		return CompletableFuture.completedFuture(null);
	}

	private ChangedPosition analyzeOperatorDifference(OperatorDescriptor self, OperatorDescriptor modified) {
		if (self.getUdf() != modified.getUdf()){
			return ChangedPosition.UDF;
		}
		return ChangedPosition.NO_CHANGE;
	}

	@Override
	@Deprecated
	public CompletableFuture<Acknowledge> updateFunction(JobGraph jobGraph, JobVertexID targetVertexID, OperatorID operatorID) {
		System.out.println("some one want to triggerOperatorUpdate using OperatorUpdateCoordinator?");
//		this.jobGraph = jobGraph;
		// By evaluating:
		// "new StreamConfig((executionGraph.tasks.values().toArray()[1]).jobVertex.configuration).getTransitiveChainedTaskConfigs(userCodeClassLoader).get(4).getStreamOperatorFactory(userCodeClassLoader).getOperator()"
		// The logic in execution Graph has been modified since now they are in same process which sharing the same reference

		// some deploy related here...
		ExecutionJobVertex executionJobVertex = executionGraph.getJobVertex(targetVertexID);
		Preconditions.checkNotNull(executionJobVertex, "can not found this execution job vertex");

		ArrayList<CompletableFuture<Void>> futures = new ArrayList<>(executionJobVertex.getTaskVertices().length);
		for (ExecutionVertex vertex : executionJobVertex.getTaskVertices()) {
			Execution execution = vertex.getCurrentExecutionAttempt();
			futures.add(execution.scheduleOperatorUpdate(operatorID));
		}
		return FutureUtils.completeAll(futures).thenApply(o -> Acknowledge.get());
	}

	enum ChangedPosition {
		UDF,
		KEY_MAPPING,
		KEY_STATE_ALLOCATION,
		PARALLELISM,
		NO_CHANGE
	}

}
