/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmanager.slots;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.TaskBackPressureResponse;
import org.apache.flink.runtime.rescale.ReconfigID;
import org.apache.flink.runtime.rescale.RescaleOptions;
import org.apache.flink.runtime.rpc.RpcTimeout;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Task manager gateway interface to communicate with the task manager.
 */
public interface TaskManagerGateway {

	/**
	 * Return the address of the task manager with which the gateway is associated.
	 *
	 * @return Address of the task manager with which this gateway is associated.
	 */
	String getAddress();

	/**
	 * Request the back pressure ratio for the given task.
	 *
	 * @param executionAttemptID identifying the task to request.
	 * @param requestId id of the request.
	 * @param timeout rpc request timeout.
	 * @return A future of the task back pressure result.
	 */
	CompletableFuture<TaskBackPressureResponse> requestTaskBackPressure(
		ExecutionAttemptID executionAttemptID,
		int requestId,
		Time timeout);

	/**
	 * Submit a task to the task manager.
	 *
	 * @param tdd describing the task to submit
	 * @param timeout of the submit operation
	 * @return Future acknowledge of the successful operation
	 */
	CompletableFuture<Acknowledge> submitTask(
		TaskDeploymentDescriptor tdd,
		Time timeout);

	CompletableFuture<Acknowledge> rescaleTask(
		ExecutionAttemptID executionAttemptID,
		TaskDeploymentDescriptor tdd,
		RescaleOptions rescaleOptions,
		Time timeout);

	CompletableFuture<Acknowledge> prepareForSync(
		ExecutionAttemptID executionAttemptID,
		int syncFlag,
		ReconfigID reconfigID,
		@RpcTimeout Time timeout);

	/**
	 * Update the operator of the given task.
	 *
	 * @param executionAttemptID identifying the task
	 * @param operatorID the id of being updated operator
	 * @param timeout of the submit operation
	 * @return Future acknowledge if the task is successfully canceled
	 */
	CompletableFuture<Acknowledge> updateOperator(
		ExecutionAttemptID executionAttemptID,
		TaskDeploymentDescriptor tdd,
		OperatorID operatorID,
		Time timeout);

	/**
	 * Cancel the given task.
	 *
	 * @param executionAttemptID identifying the task
	 * @param timeout of the submit operation
	 * @return Future acknowledge if the task is successfully canceled
	 */
	CompletableFuture<Acknowledge> cancelTask(
		ExecutionAttemptID executionAttemptID,
		Time timeout);

	/**
	 * Update the task where the given partitions can be found.
	 *
	 * @param executionAttemptID identifying the task
	 * @param partitionInfos telling where the partition can be retrieved from
	 * @param timeout of the submit operation
	 * @return Future acknowledge if the partitions have been successfully updated
	 */
	CompletableFuture<Acknowledge> updatePartitions(
		ExecutionAttemptID executionAttemptID,
		Iterable<PartitionInfo> partitionInfos,
		Time timeout);

	/**
	 * Batch release intermediate result partitions.
	 *
	 * @param jobId id of the job that the partitions belong to
	 * @param partitionIds partition ids to release
	 */
	void releasePartitions(JobID jobId, Set<ResultPartitionID> partitionIds);

	/**
	 * Notify the given task about a completed checkpoint.
	 *
	 * @param executionAttemptID identifying the task
	 * @param jobId identifying the job to which the task belongs
	 * @param checkpointId of the completed checkpoint
	 * @param timestamp of the completed checkpoint
	 */
	void notifyCheckpointComplete(
		ExecutionAttemptID executionAttemptID,
		JobID jobId,
		long checkpointId,
		long timestamp);

	/**
	 * Trigger for the given task a checkpoint.
	 *
	 * @param executionAttemptID identifying the task
	 * @param jobId identifying the job to which the task belongs
	 * @param checkpointId of the checkpoint to trigger
	 * @param timestamp of the checkpoint to trigger
	 * @param checkpointOptions of the checkpoint to trigger
	 * @param advanceToEndOfEventTime Flag indicating if the source should inject a {@code MAX_WATERMARK} in the pipeline
	 *                              to fire any registered event-time timers
	 */
	void triggerCheckpoint(
		ExecutionAttemptID executionAttemptID,
		JobID jobId,
		long checkpointId,
		long timestamp,
		CheckpointOptions checkpointOptions,
		boolean advanceToEndOfEventTime);

	/**
	 * Frees the slot with the given allocation ID.
	 *
	 * @param allocationId identifying the slot to free
	 * @param cause of the freeing operation
	 * @param timeout for the operation
	 * @return Future acknowledge which is returned once the slot has been freed
	 */
	CompletableFuture<Acknowledge> freeSlot(
		final AllocationID allocationId,
		final Throwable cause,
		@RpcTimeout final Time timeout);
}
