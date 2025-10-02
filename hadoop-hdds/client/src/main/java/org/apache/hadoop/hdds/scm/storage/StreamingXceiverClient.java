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

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.StreamReadRequest;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.StreamReadResponse;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;

/**
 * Interface for XceiverClient implementations that support streaming reads.
 * This extends the basic XceiverClient functionality with streaming capabilities
 * for efficient large data transfers.
 */
public interface StreamingXceiverClient {
  
  /**
   * Initiate a streaming read operation.
   * 
   * @param request the streaming read request containing chunk information
   * @param responseObserver observer to handle streaming responses
   * @throws IOException if the streaming operation cannot be initiated
   */
  void streamRead(StreamReadRequest request, 
                  StreamObserver<StreamReadResponse> responseObserver) 
                  throws IOException;
  
  /**
   * Check if this client supports streaming reads.
   * 
   * @return true if streaming is supported and enabled
   */
  boolean isStreamingEnabled();
  
  /**
   * Get the maximum buffer size for streaming operations.
   * 
   * @return buffer size in bytes
   */
  int getStreamingBufferSize();
}
