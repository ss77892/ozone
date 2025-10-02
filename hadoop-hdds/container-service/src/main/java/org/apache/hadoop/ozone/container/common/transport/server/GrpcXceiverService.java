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

package org.apache.hadoop.ozone.container.common.transport.server;

import static org.apache.hadoop.hdds.protocol.datanode.proto.XceiverClientProtocolServiceGrpc.getSendMethod;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.protocol.datanode.proto.XceiverClientProtocolServiceGrpc;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.ratis.grpc.util.ZeroCopyMessageMarshaller;
import org.apache.ratis.thirdparty.com.google.protobuf.MessageLite;
import org.apache.ratis.thirdparty.io.grpc.MethodDescriptor;
import org.apache.ratis.thirdparty.io.grpc.ServerCallHandler;
import org.apache.ratis.thirdparty.io.grpc.ServerServiceDefinition;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Grpc Service for handling Container Commands on datanode.
 */
public class GrpcXceiverService extends
    XceiverClientProtocolServiceGrpc.XceiverClientProtocolServiceImplBase {
  public static final Logger
      LOG = LoggerFactory.getLogger(GrpcXceiverService.class);

  private final ContainerDispatcher dispatcher;
  private final ZeroCopyMessageMarshaller<ContainerCommandRequestProto>
      zeroCopyMessageMarshaller = new ZeroCopyMessageMarshaller<>(
          ContainerCommandRequestProto.getDefaultInstance());

  public GrpcXceiverService(ContainerDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  /**
   * Bind service with zerocopy marshaller equipped for the `send` API.
   * @return  service definition.
   */
  public ServerServiceDefinition bindServiceWithZeroCopy() {
    ServerServiceDefinition orig = super.bindService();

    ServerServiceDefinition.Builder builder =
        ServerServiceDefinition.builder(orig.getServiceDescriptor().getName());
    // Add `send` method with zerocopy marshaller.
    addZeroCopyMethod(orig, builder, getSendMethod(),
        zeroCopyMessageMarshaller);
    // Add other methods as is.
    orig.getMethods().stream().filter(
        x -> !x.getMethodDescriptor().getFullMethodName().equals(
            getSendMethod().getFullMethodName())
    ).forEach(
        builder::addMethod
    );

    return builder.build();
  }

  private static <Req extends MessageLite, Resp> void addZeroCopyMethod(
      ServerServiceDefinition orig,
      ServerServiceDefinition.Builder newServiceBuilder,
      MethodDescriptor<Req, Resp> origMethod,
      ZeroCopyMessageMarshaller<Req> zeroCopyMarshaller) {
    MethodDescriptor<Req, Resp> newMethod = origMethod.toBuilder()
        .setRequestMarshaller(zeroCopyMarshaller)
        .build();
    @SuppressWarnings("unchecked")
    ServerCallHandler<Req, Resp> serverCallHandler =
        (ServerCallHandler<Req, Resp>) orig.getMethod(
            newMethod.getFullMethodName()).getServerCallHandler();
    newServiceBuilder.addMethod(newMethod, serverCallHandler);
  }

  @Override
  public StreamObserver<ContainerCommandRequestProto> send(
      StreamObserver<ContainerCommandResponseProto> responseObserver) {
    return new StreamObserver<ContainerCommandRequestProto>() {
      private final AtomicBoolean isClosed = new AtomicBoolean(false);

      @Override
      public void onNext(ContainerCommandRequestProto request) {
        final DispatcherContext context = request.getCmdType() != Type.ReadChunk ? null
            : DispatcherContext.newBuilder(DispatcherContext.Op.HANDLE_READ_CHUNK)
            .setReleaseSupported(true)
            .build();

        try {
          final ContainerCommandResponseProto resp = dispatcher.dispatch(request, context);
          responseObserver.onNext(resp);
        } catch (Throwable e) {
          LOG.error("Got exception when processing"
                    + " ContainerCommandRequestProto {}", request, e);
          isClosed.set(true);
          responseObserver.onError(e);
        } finally {
          zeroCopyMessageMarshaller.release(request);
          if (context != null) {
            context.release();
          }
        }
      }

      @Override
      public void onError(Throwable t) {
        // for now we just log a msg
        LOG.error("ContainerCommand send on error. Exception: ", t);
      }

      @Override
      public void onCompleted() {
        if (isClosed.compareAndSet(false, true)) {
          LOG.debug("ContainerCommand send completed");
          responseObserver.onCompleted();
        }
      }
    };
  }
  
  // Streaming read implementation
  @Override
  public void streamRead(org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.StreamReadRequest request, 
                        org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver<org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.StreamReadResponse> responseObserver) {
    LOG.info("Streaming read request received for {} chunks in block {}", 
             request.getChunksCount(), request.getBlockID().getLocalID());
    
    try {
      for (org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo chunkInfo : request.getChunksList()) {
        // Create individual read chunk request for each chunk
        org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto readRequest = 
            org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto.newBuilder()
                .setCmdType(org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type.ReadChunk)
                .setContainerID(request.getBlockID().getContainerID())
                .setDatanodeUuid("")
                .setReadChunk(org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadChunkRequestProto.newBuilder()
                    .setBlockID(request.getBlockID())
                    .setChunkData(chunkInfo)
                    .setReadChunkVersion(request.getReadChunkVersion())
                    .build())
                .build();
        
        // Dispatch the read request
        DispatcherContext context = DispatcherContext.newBuilder(
            DispatcherContext.Op.HANDLE_READ_CHUNK)
            .setReleaseSupported(true)
            .build();
        
        try {
          org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto response = dispatcher.dispatch(readRequest, context);
          
          if (response.getResult() == org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.SUCCESS) {
            org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadChunkResponseProto readResponse = response.getReadChunk();
            
            // Create streaming response
            org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.StreamReadResponse.Builder streamResponse = org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.StreamReadResponse.newBuilder()
                .setBlockID(request.getBlockID())
                .setChunkData(chunkInfo)
                .setOffset(chunkInfo.getOffset())
                .setLength((int) chunkInfo.getLen());
            
            // Add data from response
            if (readResponse.hasData()) {
              streamResponse.setData(readResponse.getData());
            } else if (readResponse.hasDataBuffers()) {
              // Concatenate multiple buffers into single ByteString
              org.apache.ratis.thirdparty.com.google.protobuf.ByteString.Output output = org.apache.ratis.thirdparty.com.google.protobuf.ByteString.newOutput();
              for (org.apache.ratis.thirdparty.com.google.protobuf.ByteString buffer : readResponse.getDataBuffers().getBuffersList()) {
                buffer.writeTo(output);
              }
              streamResponse.setData(output.toByteString());
            }
            
            responseObserver.onNext(streamResponse.build());
            
          } else {
            org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.StreamReadResponse errorResponse = org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.StreamReadResponse.newBuilder()
                .setBlockID(request.getBlockID())
                .setChunkData(chunkInfo)
                .setError("Failed to read chunk: " + response.getMessage())
                .build();
            responseObserver.onNext(errorResponse);
          }
          
        } catch (Exception e) {
          LOG.error("Error reading chunk {} from block {}", 
                   chunkInfo.getChunkName(), request.getBlockID().getLocalID(), e);
          org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.StreamReadResponse errorResponse = org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.StreamReadResponse.newBuilder()
              .setBlockID(request.getBlockID())
              .setChunkData(chunkInfo)
              .setError("Exception reading chunk: " + e.getMessage())
              .build();
          responseObserver.onNext(errorResponse);
        } finally {
          if (context != null) {
            context.release();
          }
        }
      }
      
      responseObserver.onCompleted();
      LOG.info("Streaming read completed for block {}", request.getBlockID().getLocalID());
      
    } catch (Exception e) {
      LOG.error("Streaming read failed for block {}", request.getBlockID().getLocalID(), e);
      responseObserver.onError(e);
    }
  }
}
