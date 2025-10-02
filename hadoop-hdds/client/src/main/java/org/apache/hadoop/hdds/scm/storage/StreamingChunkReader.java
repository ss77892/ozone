/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DatanodeBlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.StreamReadRequest;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.StreamReadResponse;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for streaming chunk reads from DataNodes.
 * Manages the asynchronous streaming of chunk data and provides
 * a blocking interface for sequential reading.
 */
public class StreamingChunkReader implements StreamObserver<StreamReadResponse> {
  
  private static final Logger LOG = LoggerFactory.getLogger(StreamingChunkReader.class);
  
  private final BlockingQueue<StreamReadResponse> responseQueue;
  private final AtomicBoolean completed;
  private final AtomicBoolean failed;
  private final AtomicReference<Throwable> error;
  private final XceiverClientSpi xceiverClient;
  private final DatanodeBlockID blockID;
  private final int bufferSize;
  
  // Timeout for waiting on streaming responses
  private static final int STREAM_TIMEOUT_SECONDS = 30;
  
  public StreamingChunkReader(XceiverClientSpi xceiverClient, 
                             DatanodeBlockID blockID, 
                             int bufferSize) {
    this.xceiverClient = xceiverClient;
    this.blockID = blockID;
    this.bufferSize = bufferSize;
    this.responseQueue = new LinkedBlockingQueue<>();
    this.completed = new AtomicBoolean(false);
    this.failed = new AtomicBoolean(false);
    this.error = new AtomicReference<>();
  }
  
  /**
   * Initiate streaming read for multiple chunks.
   * 
   * @param chunks List of chunks to read
   * @throws IOException if streaming initiation fails
   */
  public void startStreaming(List<ChunkInfo> chunks) throws IOException {
    if (!(xceiverClient instanceof StreamingXceiverClient)) {
      throw new IOException("Client does not support streaming reads");
    }
    
    StreamingXceiverClient streamingClient = (StreamingXceiverClient) xceiverClient;
    
    StreamReadRequest.Builder requestBuilder = StreamReadRequest.newBuilder()
        .setBlockID(blockID)
        .setBufferSize(bufferSize)
        .setVerifyChecksum(true);
    
    for (ChunkInfo chunk : chunks) {
      requestBuilder.addChunks(chunk);
    }
    
    StreamReadRequest request = requestBuilder.build();
    
    LOG.info("Starting streaming read for {} chunks in block {}", 
             chunks.size(), blockID.toString());
    
    streamingClient.streamRead(request, this);
  }
  
  /**
   * Get the next chunk data from the stream.
   * Blocks until data is available or stream ends.
   * 
   * @return ByteBuffer containing chunk data, or null if stream ended
   * @throws IOException if stream failed or timeout occurred
   */
  public ByteBuffer readNext() throws IOException {
    if (failed.get()) {
      Throwable cause = error.get();
      throw new IOException("Streaming read failed", cause);
    }
    
    if (completed.get() && responseQueue.isEmpty()) {
      return null; // Stream ended
    }
    
    try {
      StreamReadResponse response = responseQueue.poll(STREAM_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      
      if (response == null) {
        if (failed.get()) {
          throw new IOException("Streaming read failed", error.get());
        }
        throw new IOException("Timeout waiting for streaming response");
      }
      
      if (response.hasError()) {
        throw new IOException("DataNode streaming error: " + response.getError());
      }
      
      ByteBuffer buffer = ByteBuffer.wrap(response.getData().toByteArray());
      
      LOG.debug("Received streaming chunk: {} bytes for chunk {}", 
                buffer.remaining(), response.getChunkData().getChunkName());
      
      return buffer;
      
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while reading stream", e);
    }
  }
  
  /**
   * Check if there are more responses available.
   */
  public boolean hasNext() {
    return !responseQueue.isEmpty() || (!completed.get() && !failed.get());
  }
  
  /**
   * Close the streaming reader and clean up resources.
   */
  public void close() {
    completed.set(true);
    responseQueue.clear();
  }
  
  // StreamObserver implementation
  
  @Override
  public void onNext(StreamReadResponse response) {
    try {
      responseQueue.offer(response);
      LOG.debug("Queued streaming response for chunk: {}", 
                response.getChunkData().getChunkName());
    } catch (Exception e) {
      LOG.error("Error processing streaming response", e);
      onError(e);
    }
  }
  
  @Override
  public void onError(Throwable throwable) {
    LOG.error("Streaming read failed for block {}", blockID.toString(), throwable);
    failed.set(true);
    error.set(throwable);
    
    // Wake up any blocked readers
    StreamReadResponse errorResponse = StreamReadResponse.newBuilder()
        .setBlockID(blockID)
        .setError(throwable.getMessage())
        .build();
    responseQueue.offer(errorResponse);
  }
  
  @Override
  public void onCompleted() {
    LOG.info("Streaming read completed for block {}", blockID.toString());
    completed.set(true);
  }
}
