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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.runtime.rescale.reconfigure.TaskOperatorManager;
import org.apache.flink.runtime.util.profiling.MetricsManager;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Implementation of {@link StreamTaskInput} that wraps an input from network taken from {@link CheckpointedInputGate}.
 *
 * <p>This internally uses a {@link StatusWatermarkValve} to keep track of {@link Watermark} and
 * {@link StreamStatus} events, and forwards them to event subscribers once the
 * {@link StatusWatermarkValve} determines the {@link Watermark} from all inputs has advanced, or
 * that a {@link StreamStatus} needs to be propagated downstream to denote a status change.
 *
 * <p>Forwarding elements, watermarks, or status status elements must be protected by synchronizing
 * on the given lock object. This ensures that we don't call methods on a
 * {@link StreamInputProcessor} concurrently with the timer callback or other things.
 */
@Internal
public final class StreamTaskNetworkInput<T> implements StreamTaskInput<T> {

	private final CheckpointedInputGate checkpointedInputGate;

	private final DeserializationDelegate<StreamElement> deserializationDelegate;

	private RecordDeserializer<DeserializationDelegate<StreamElement>>[] recordDeserializers;

	/** Valve that controls how watermarks and stream statuses are forwarded. */
	private final StatusWatermarkValve statusWatermarkValve;

	private final int inputIndex;

	private int lastChannel = UNSPECIFIED;

	private RecordDeserializer<DeserializationDelegate<StreamElement>> currentRecordDeserializer = null;

	private final IOManager ioManager;

	private MetricsManager metricsManager;

	private TaskOperatorManager.PauseActionController pauseActionController;

	@SuppressWarnings("unchecked")
	public StreamTaskNetworkInput(
			CheckpointedInputGate checkpointedInputGate,
			TypeSerializer<?> inputSerializer,
			IOManager ioManager,
			StatusWatermarkValve statusWatermarkValve,
			int inputIndex) {
		this.checkpointedInputGate = checkpointedInputGate;
		this.deserializationDelegate = new NonReusingDeserializationDelegate<>(
			new StreamElementSerializer<>(inputSerializer));

		// Initialize one deserializer per input channel
		this.recordDeserializers = new SpillingAdaptiveSpanningRecordDeserializer[checkpointedInputGate.getNumberOfInputChannels()];
		for (int i = 0; i < recordDeserializers.length; i++) {
			recordDeserializers[i] = new SpillingAdaptiveSpanningRecordDeserializer<>(
				ioManager.getSpillingDirectoriesPaths());
		}

		this.statusWatermarkValve = checkNotNull(statusWatermarkValve);
		this.inputIndex = inputIndex;

		this.ioManager = ioManager;
	}

	@VisibleForTesting
	StreamTaskNetworkInput(
		CheckpointedInputGate checkpointedInputGate,
		TypeSerializer<?> inputSerializer,
		StatusWatermarkValve statusWatermarkValve,
		int inputIndex,
		RecordDeserializer<DeserializationDelegate<StreamElement>>[] recordDeserializers) {

		this.checkpointedInputGate = checkpointedInputGate;
		this.deserializationDelegate = new NonReusingDeserializationDelegate<>(
			new StreamElementSerializer<>(inputSerializer));
		this.recordDeserializers = recordDeserializers;
		this.statusWatermarkValve = statusWatermarkValve;
		this.inputIndex = inputIndex;

		this.ioManager = null;
	}

	public void setPauseActionController(TaskOperatorManager.PauseActionController pauseActionController) {
		this.pauseActionController = pauseActionController;
	}

	@Override
	public InputStatus emitNext(DataOutput<T> output) throws Exception {

		while (true) {
			// get the stream element from the deserializer
			if (currentRecordDeserializer != null) {
				DeserializationResult result = currentRecordDeserializer.getNextRecord(deserializationDelegate);
				if (result.isBufferConsumed()) {
					currentRecordDeserializer.getCurrentBuffer().recycleBuffer();
					currentRecordDeserializer = null;
				}

				if (result.isFullRecord()) {
					processElement(deserializationDelegate.getInstance(), output);
					return InputStatus.MORE_AVAILABLE;
				}
			}

			Optional<BufferOrEvent> bufferOrEvent = checkpointedInputGate.pollNext();
			if (bufferOrEvent.isPresent()) {
				processBufferOrEvent(bufferOrEvent.get());
			} else {
				if (checkpointedInputGate.isFinished()) {
					checkState(checkpointedInputGate.getAvailableFuture().isDone(), "Finished BarrierHandler should be available");
					if (!checkpointedInputGate.isEmpty()) {
						throw new IllegalStateException("Trailing data in checkpoint barrier handler.");
					}
					return InputStatus.END_OF_INPUT;
				}
				if(this.pauseActionController != null && pauseActionController.ackIfPause()){
					return InputStatus.NEED_PAUSE;
				}
				return InputStatus.NOTHING_AVAILABLE;
			}
		}
	}

	private void processElement(StreamElement recordOrMark, DataOutput<T> output) throws Exception {
		if (recordOrMark.isRecord()){
			output.emitRecord(recordOrMark.asRecord());
		} else if (recordOrMark.isWatermark()) {
			statusWatermarkValve.inputWatermark(recordOrMark.asWatermark(), lastChannel);
		} else if (recordOrMark.isLatencyMarker()) {
			output.emitLatencyMarker(recordOrMark.asLatencyMarker());
		} else if (recordOrMark.isStreamStatus()) {
			statusWatermarkValve.inputStreamStatus(recordOrMark.asStreamStatus(), lastChannel);
		} else {
			throw new UnsupportedOperationException("Unknown type of StreamElement");
		}
	}

	private void processBufferOrEvent(BufferOrEvent bufferOrEvent) throws IOException {
		if (bufferOrEvent.isBuffer()) {
			lastChannel = bufferOrEvent.getChannelIndex();
			checkState(lastChannel != StreamTaskInput.UNSPECIFIED);
			currentRecordDeserializer = recordDeserializers[lastChannel];
			checkState(currentRecordDeserializer != null,
				"currentRecordDeserializer has already been released");

			currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());
		}
		else {
			// Event received
			final AbstractEvent event = bufferOrEvent.getEvent();
			// TODO: with checkpointedInputGate.isFinished() we might not need to support any events on this level.
			if (event.getClass() != EndOfPartitionEvent.class) {
				throw new IOException("Unexpected event: " + event);
			}

			// release the record deserializer immediately,
			// which is very valuable in case of bounded stream
			releaseDeserializer(bufferOrEvent.getChannelIndex());
		}
	}

	@Override
	public int getInputIndex() {
		return inputIndex;
	}

	@Override
	public CompletableFuture<?> getAvailableFuture() {
		if (currentRecordDeserializer != null) {
			return AVAILABLE;
		}
		return checkpointedInputGate.getAvailableFuture();
	}

	@Override
	public void close() throws IOException {
		// release the deserializers . this part should not ever fail
		for (int channelIndex = 0; channelIndex < recordDeserializers.length; channelIndex++) {
			releaseDeserializer(channelIndex);
		}

		// cleanup the resources of the checkpointed input gate
		checkpointedInputGate.cleanup();
	}

	private void releaseDeserializer(int channelIndex) {
		RecordDeserializer<?> deserializer = recordDeserializers[channelIndex];
		if (deserializer != null) {
			// recycle buffers and clear the deserializer.
			Buffer buffer = deserializer.getCurrentBuffer();
			if (buffer != null && !buffer.isRecycled()) {
				buffer.recycleBuffer();
			}
			deserializer.clear();

			recordDeserializers[channelIndex] = null;
		}
	}

	public void reconnect() {
		// TODO: if the num of channel is the same, do we need to update all those stuff?
		int numInputChannels = checkpointedInputGate.getNumberOfInputChannels();

		RecordDeserializer<DeserializationDelegate<StreamElement>>[] oldDeserializer =
			Arrays.copyOf(recordDeserializers, recordDeserializers.length);
		recordDeserializers = new SpillingAdaptiveSpanningRecordDeserializer[numInputChannels];

		for (int i = 0; i < recordDeserializers.length; i++) {
			if (i < oldDeserializer.length) {
				recordDeserializers[i] = oldDeserializer[i];
			} else {
				recordDeserializers[i] = new SpillingAdaptiveSpanningRecordDeserializer<>(
					ioManager.getSpillingDirectoriesPaths());
			}
		}

		statusWatermarkValve.rescale(numInputChannels);
		checkpointedInputGate.updateHandlerBufferChannels(numInputChannels);
	}

	public void setMetricsManager(MetricsManager metricsManager) {
		this.metricsManager = metricsManager;
	}
}
