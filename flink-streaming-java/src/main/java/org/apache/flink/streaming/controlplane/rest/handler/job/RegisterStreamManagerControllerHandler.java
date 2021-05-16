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

package org.apache.flink.streaming.controlplane.rest.handler.job;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.*;
import org.apache.flink.runtime.rest.messages.job.*;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.streaming.controlplane.rest.handler.AbstractStreamManagerRestHandler;
import org.apache.flink.streaming.controlplane.webmonitor.StreamManagerRestfulGateway;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FileUtils;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

/**
 * Returns the {@link org.apache.flink.api.common.JobExecutionResult} for a given {@link JobID}.
 */
public class RegisterStreamManagerControllerHandler
	extends AbstractStreamManagerRestHandler<StreamManagerRestfulGateway, SubmitControllerRequestBody, EmptyResponseBody, JobMessageParameters> {

	public RegisterStreamManagerControllerHandler(
		final GatewayRetriever<? extends StreamManagerRestfulGateway> leaderRetriever,
		final Time timeout,
		final Map<String, String> responseHeaders) {
		super(
			leaderRetriever,
			timeout,
			responseHeaders,
			StreamManagerControllerHeaders.getInstance());
	}

	@Override
	protected CompletableFuture<EmptyResponseBody> handleRequest(
		@Nonnull final HandlerRequest<SubmitControllerRequestBody, JobMessageParameters> request,
		@Nonnull final StreamManagerRestfulGateway gateway) throws RestHandlerException {

		final JobID jobId = request.getPathParameter(JobIDPathParameter.class);

		final CompletableFuture<JobStatus> jobStatusFuture = gateway.requestJobStatus(jobId, timeout);

		final SubmitControllerRequestBody requestBody = request.getRequestBody();

		String className = requestBody.controllerClassName;
		String classFileName = requestBody.classFile;

		final Collection<File> uploadedFiles = request.getUploadedFiles();
		final Map<String, Path> nameToFile = uploadedFiles.stream().collect(Collectors.toMap(
			File::getName,
			Path::fromLocalFile
		));
		return jobStatusFuture.thenCompose(
			jobStatus -> {
				if (jobStatus.isGloballyTerminalState()) {
					return sendControllerToGateway(gateway, jobId, className, classFileName, nameToFile);
				} else {
					return CompletableFuture.completedFuture(
						EmptyResponseBody.getInstance());
				}
			}).exceptionally(throwable -> {
			throw propagateException(throwable);
		});
	}

	private CompletableFuture<EmptyResponseBody> sendControllerToGateway(
		@Nonnull StreamManagerRestfulGateway gateway,
		JobID jobId,
		String className,
		String classFileName,
		Map<String, Path> nameToFile) {

		Path classFile = nameToFile.get(classFileName);
		String sourceCode = null;
		try {
			sourceCode = FileUtils.readFileUtf8(new File(classFile.getPath()));
		} catch (IOException e) {
			e.printStackTrace();
		}
		//todo with null source code
		return gateway
			.registerNewController(jobId, "DEFAULT", className, sourceCode, timeout)
			.thenApply(r -> EmptyResponseBody.getInstance());
	}

	private static CompletionException propagateException(final Throwable throwable) {
		final Throwable cause = ExceptionUtils.stripCompletionException(throwable);

		if (cause instanceof FlinkJobNotFoundException) {
			throw new CompletionException(new RestHandlerException(
				throwable.getMessage(),
				HttpResponseStatus.NOT_FOUND,
				throwable));
		} else {
			throw new CompletionException(throwable);
		}
	}
}
