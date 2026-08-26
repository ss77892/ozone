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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadBlockResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.StreamingReadResponse;
import org.apache.hadoop.hdds.scm.StreamingReaderSpi;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.hadoop.security.token.Token;
import org.apache.ratis.protocol.exceptions.TimeoutIOException;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException;
import org.apache.ratis.thirdparty.io.grpc.stub.ClientCallStreamObserver;
import org.junit.jupiter.api.Test;

/**
 * Tests for StreamBlockInputStream custom configuration behavior.
 */
public class TestStreamBlockInputStream {

  private static final Duration STREAM_READ_TIMEOUT = Duration.ofSeconds(5);
  private static final Function<BlockID, BlockLocationInfo> NO_REFRESH = b -> null;

  @Test
  public void testCustomStreamReadConfigIsApplied() throws Exception {
    // Arrange: create a config with non-default values
    OzoneClientConfig clientConfig = newStreamReadConfig();
    clientConfig.setStreamReadPreReadSize(64L << 20);
    clientConfig.setStreamReadResponseDataSize(2 << 20);

    // Sanity check
    assertEquals(STREAM_READ_TIMEOUT, clientConfig.getStreamReadTimeout());
    // Create a dummy BlockID for the test
    BlockID blockID = new BlockID(1L, 1L);
    long length = 1024L;
    // Create a mock Pipeline instance.
    Pipeline pipeline = mock(Pipeline.class);

    Token<OzoneBlockTokenIdentifier> token = null;
    // Mock XceiverClientFactory since StreamBlockInputStream requires it in the constructor
    XceiverClientFactory xceiverClientFactory = mock(XceiverClientFactory.class);
    // Create a StreamBlockInputStream instance
    try (StreamBlockInputStream sbis = new StreamBlockInputStream(
        blockID, length, pipeline, token,
        xceiverClientFactory, NO_REFRESH, clientConfig)) {

      // Assert: fields should match config values
      assertEquals(64L << 20, sbis.getPreReadSize());
      assertEquals(2 << 20, sbis.getResponseDataSize());
      assertEquals(STREAM_READ_TIMEOUT, sbis.getReadTimeout());
    }
  }

  @Test
  public void testReleasesStreamPermitAtBlockEof() throws Exception {
    OzoneClientConfig clientConfig = newStreamReadConfig();
    BlockID blockID = new BlockID(1L, 2L);
    byte[] data = new byte[] {1, 2, 3, 4};
    long length = data.length;
    Pipeline pipeline = mockStandalonePipeline();
    ClientCallStreamObserver<ContainerCommandRequestProto> requestObserver = mock(ClientCallStreamObserver.class);
    XceiverClientGrpc xceiverClient = mockStreamingReadClient(data, requestObserver);
    XceiverClientFactory xceiverClientFactory = mock(XceiverClientFactory.class);
    when(xceiverClientFactory.acquireClientForReadData(any(Pipeline.class)))
        .thenReturn(xceiverClient);

    try (StreamBlockInputStream sbis = new StreamBlockInputStream(
        blockID, length, pipeline, null, xceiverClientFactory,
        NO_REFRESH, clientConfig)) {
      ByteBuffer firstRead = ByteBuffer.allocate((int) length - 1);
      int first = sbis.read(firstRead);
      assertEquals(length - 1, first);
      assertEquals(length - 1, sbis.getPos());
      verify(xceiverClient, never()).completeStreamRead();

      int last = sbis.read();
      assertEquals(data[(int) length - 1] & 0xFF, last);
      assertEquals(length, sbis.getPos());
      verify(xceiverClient, times(1)).completeStreamRead();
      verify(requestObserver, times(1)).onCompleted();

      // Subsequent reads should return EOF and must not trigger duplicate permit release.
      assertEquals(-1, sbis.read());
      assertEquals(-1, sbis.read());
    }

    verify(xceiverClient, times(1)).completeStreamRead();
    verify(requestObserver, times(1)).onCompleted();
    verify(requestObserver, never()).cancel(any(), any());
  }

  @Test
  public void testCancelsRequestStreamWhenOnCompletedThrows() throws Exception {
    OzoneClientConfig clientConfig = newStreamReadConfig();
    BlockID blockID = new BlockID(1L, 3L);
    byte[] data = new byte[] {1, 2, 3, 4};
    Pipeline pipeline = mockStandalonePipeline();
    ClientCallStreamObserver<ContainerCommandRequestProto> requestObserver = mock(ClientCallStreamObserver.class);
    RuntimeException closeFailure = new RuntimeException("close failed");
    doThrow(closeFailure).when(requestObserver).onCompleted();

    XceiverClientGrpc xceiverClient = mockStreamingReadClient(data, requestObserver);
    XceiverClientFactory xceiverClientFactory = mock(XceiverClientFactory.class);
    when(xceiverClientFactory.acquireClientForReadData(any(Pipeline.class))).thenReturn(xceiverClient);

    try (StreamBlockInputStream sbis = new StreamBlockInputStream(
        blockID, data.length, pipeline, null, xceiverClientFactory, NO_REFRESH, clientConfig)) {
      ByteBuffer all = ByteBuffer.allocate(data.length);
      assertEquals(data.length, sbis.read(all));
      assertEquals(data.length, sbis.getPos());
      assertEquals(-1, sbis.read());
    }

    verify(requestObserver, times(1)).onCompleted();
    verify(requestObserver, times(1)).cancel(eq("StreamBlockInputStream closed"), eq(closeFailure));
    verify(xceiverClient, times(1)).completeStreamRead();
  }

  @Test
  public void testCloseDoesNotFailWhenOnCompletedAndCancelThrow() throws Exception {
    OzoneClientConfig clientConfig = newStreamReadConfig();
    BlockID blockID = new BlockID(1L, 4L);
    byte[] data = new byte[] {1, 2, 3, 4};
    Pipeline pipeline = mockStandalonePipeline();
    ClientCallStreamObserver<ContainerCommandRequestProto> requestObserver = mock(ClientCallStreamObserver.class);
    RuntimeException closeFailure = new RuntimeException("close failed");
    RuntimeException cancelFailure = new RuntimeException("cancel failed");
    doThrow(closeFailure).when(requestObserver).onCompleted();
    doThrow(cancelFailure).when(requestObserver)
        .cancel(eq("StreamBlockInputStream closed"), eq(closeFailure));

    XceiverClientGrpc xceiverClient = mockStreamingReadClient(data, requestObserver);
    XceiverClientFactory xceiverClientFactory = mock(XceiverClientFactory.class);
    when(xceiverClientFactory.acquireClientForReadData(any(Pipeline.class))).thenReturn(xceiverClient);

    try (StreamBlockInputStream sbis = new StreamBlockInputStream(
        blockID, data.length, pipeline, null, xceiverClientFactory, NO_REFRESH, clientConfig)) {
      ByteBuffer all = ByteBuffer.allocate(data.length);
      assertEquals(data.length, sbis.read(all));
      assertEquals(data.length, sbis.getPos());
      assertEquals(-1, sbis.read());
    }

    verify(requestObserver, times(1)).onCompleted();
    verify(requestObserver, times(1)).cancel(eq("StreamBlockInputStream closed"), eq(closeFailure));
    verify(xceiverClient, times(1)).completeStreamRead();
  }

  /**
   * Reproduces Bug 2: poll() checks future.isDone() before draining the queue.
   *
   * When the server delivers a response (onNext) and immediately closes the stream
   * (onCompleted) — which can happen on the same gRPC thread in rapid succession —
   * the item is in the queue and the future is already complete by the time poll()
   * first runs. poll() sees isDone()==true and returns null without ever checking
   * the queue, so readFromQueue() throws NullPointerException on the null proto.
   *
   * This test will FAIL with NullPointerException on the current code and should
   * PASS once the bug is fixed (poll must drain the queue before checking isDone).
   */
  @Test
  public void testPollDoesNotDropQueuedItemWhenFutureCompletesFirst() throws Exception {
    OzoneClientConfig clientConfig = newStreamReadConfig();
    BlockID blockID = new BlockID(1L, 10L);
    byte[] data = {1, 2, 3, 4};
    Pipeline pipeline = mockStandalonePipeline();
    ClientCallStreamObserver<ContainerCommandRequestProto> requestObserver =
        mock(ClientCallStreamObserver.class);
    StreamingReadResponse streamingReadResponse = mock(StreamingReadResponse.class);
    when(streamingReadResponse.getRequestObserver()).thenReturn(requestObserver);

    // Capture the StreamingReaderSpi during initStreamRead so we can drive
    // its callbacks from the streamRead mock below.
    AtomicReference<StreamingReaderSpi> readerRef = new AtomicReference<>();
    XceiverClientGrpc xceiverClient = mock(XceiverClientGrpc.class);
    doAnswer(inv -> {
      StreamingReaderSpi reader = inv.getArgument(1);
      reader.setStreamingReadResponse(streamingReadResponse);
      readerRef.set(reader);
      return null;
    }).when(xceiverClient).initStreamRead(any(BlockID.class), any(), any());

    // Simulate the race: when the client sends a ReadBlock request, the server
    // responds with data (onNext) and closes the stream (onCompleted) before
    // poll() has had a chance to run — both callbacks fire on the same call stack
    // before streamRead() returns. This means when poll() is entered, the queue
    // already has the response item AND future.isDone() is already true.
    // poll() checks isDone() first and returns null, dropping the queued item.
    doAnswer(inv -> {
      StreamingReaderSpi reader = readerRef.get();
      reader.onNext(ContainerCommandResponseProto.newBuilder()
          .setCmdType(Type.ReadBlock)
          .setResult(ContainerProtos.Result.SUCCESS)
          .setReadBlock(buildReadBlockResponse(data))
          .build());
      reader.onCompleted(); // future is now done; item is already in the queue
      return null;
    }).when(xceiverClient).streamRead(any(), any());

    XceiverClientFactory xceiverClientFactory = mock(XceiverClientFactory.class);
    when(xceiverClientFactory.acquireClientForReadData(any(Pipeline.class)))
        .thenReturn(xceiverClient);

    try (StreamBlockInputStream sbis = new StreamBlockInputStream(
        blockID, data.length, pipeline, null, xceiverClientFactory,
        NO_REFRESH, clientConfig)) {

      ByteBuffer buf = ByteBuffer.allocate(data.length);
      // With the bug: poll() returns null (future done, queue unchecked) and
      // readFromQueue() throws NullPointerException.
      // After the fix: all 4 bytes are returned successfully.
      assertDoesNotThrow(() -> sbis.read(buf), "should not NPE when onCompleted fires before poll");
      assertEquals(data.length, buf.position(), "all bytes should be read");
    }
  }

  private OzoneClientConfig newStreamReadConfig() {
    OzoneClientConfig clientConfig = new OzoneClientConfig();
    clientConfig.setChecksumVerify(false);
    clientConfig.setStreamReadPreReadSize(0);
    clientConfig.setStreamReadResponseDataSize(1024);
    clientConfig.setStreamReadTimeout(STREAM_READ_TIMEOUT);
    return clientConfig;
  }

  private Pipeline mockStandalonePipeline() throws Exception {
    Pipeline pipeline = mock(Pipeline.class);
    DatanodeDetails datanode = mock(DatanodeDetails.class);

    when(pipeline.getNodes()).thenReturn(Collections.singletonList(datanode));
    when(pipeline.getNodesInOrder()).thenReturn(Collections.singletonList(datanode));
    when(pipeline.getFirstNode()).thenReturn(datanode);
    when(pipeline.getClosestNode()).thenReturn(datanode);
    when(pipeline.getType()).thenReturn(HddsProtos.ReplicationType.STAND_ALONE);
    when(pipeline.getReplicaIndex(datanode)).thenReturn(1);
    when(datanode.getID()).thenReturn(mock(DatanodeID.class));
    when(datanode.getUuidString()).thenReturn("00000000-0000-0000-0000-000000000001");

    return pipeline;
  }

  private XceiverClientGrpc mockStreamingReadClient(byte[] data,
      ClientCallStreamObserver<ContainerCommandRequestProto> requestObserver) throws Exception {
    XceiverClientGrpc xceiverClient = mock(XceiverClientGrpc.class);
    StreamingReadResponse streamingReadResponse = mock(StreamingReadResponse.class);
    ReadBlockResponseProto readBlock = buildReadBlockResponse(data);
    when(streamingReadResponse.getRequestObserver()).thenReturn(requestObserver);

    doNothing().when(xceiverClient)
        .streamRead(any(ContainerCommandRequestProto.class),
            any(StreamingReadResponse.class));
    doAnswer(invocation -> {
      StreamingReaderSpi reader = invocation.getArgument(1);
      reader.setStreamingReadResponse(streamingReadResponse);
      reader.onNext(ContainerCommandResponseProto.newBuilder()
          .setCmdType(Type.ReadBlock)
          .setResult(ContainerProtos.Result.SUCCESS)
          .setReadBlock(readBlock)
          .build());
      return null;
    }).when(xceiverClient).initStreamRead(any(BlockID.class), any(), any());

    return xceiverClient;
  }

  /**
   * Realistic test for the checksum-alignment skip path.
   *
   * After a seek, the server aligns its response to the nearest checksum boundary,
   * which may be before the client's current position. With a small responseDataSize
   * (4 bytes), the server sends two 4-byte chunks:
   *   chunk 1: blockOffset=0, data=[0,1,2,3] — entirely before seek position 4
   *   chunk 2: blockOffset=4, data=[4,5,6,7] — starts at seek position
   *
   * The while(true) loop in read() must:
   *   iteration 1: receive chunk 1, skip all 4 bytes (pos-blockOffset=4 == data.size()),
   *                empty buffer → continue
   *   iteration 2: receive chunk 2, no skip needed → return buffer with [4,5,6,7]
   *
   * This was an infinite loop or MPE before fixes in the PR that added this test.
   */
  @Test
  public void testSeekReadsCorrectBytesWhenFirstResponseIsFullyBeforePosition() throws Exception {
    OzoneClientConfig clientConfig = newStreamReadConfig();
    clientConfig.setStreamReadResponseDataSize(4); // 4-byte chunks match the test data
    BlockID blockID = new BlockID(1L, 12L);
    long length = 8;
    Pipeline pipeline = mockStandalonePipeline();
    ClientCallStreamObserver<ContainerCommandRequestProto> requestObserver =
        mock(ClientCallStreamObserver.class);
    StreamingReadResponse streamingReadResponse = mock(StreamingReadResponse.class);
    when(streamingReadResponse.getRequestObserver()).thenReturn(requestObserver);

    AtomicReference<StreamingReaderSpi> readerRef = new AtomicReference<>();
    XceiverClientGrpc xceiverClient = mock(XceiverClientGrpc.class);
    doAnswer(inv -> {
      StreamingReaderSpi reader = inv.getArgument(1);
      reader.setStreamingReadResponse(streamingReadResponse);
      readerRef.set(reader);
      return null;
    }).when(xceiverClient).initStreamRead(any(BlockID.class), any(), any());

    // Server aligns to checksum boundary 0 and sends two 4-byte responses.
    // The first chunk (bytes 0–3) is entirely before seek position 4 and will be
    // fully skipped. The second chunk (bytes 4–7) starts at our position.
    doAnswer(inv -> {
      StreamingReaderSpi reader = readerRef.get();
      reader.onNext(buildResponseProto(new byte[]{0, 1, 2, 3}, 0)); // fully skipped
      reader.onNext(buildResponseProto(new byte[]{4, 5, 6, 7}, 4)); // has our data
      reader.onCompleted();
      return null;
    }).when(xceiverClient).streamRead(any(), any());

    XceiverClientFactory xceiverClientFactory = mock(XceiverClientFactory.class);
    when(xceiverClientFactory.acquireClientForReadData(any(Pipeline.class)))
        .thenReturn(xceiverClient);

    try (StreamBlockInputStream sbis = new StreamBlockInputStream(
        blockID, length, pipeline, null, xceiverClientFactory,
        NO_REFRESH, clientConfig)) {

      sbis.seek(4);

      byte[] out = new byte[4];
      int bytesRead = sbis.read(out, 0, 4);
      assertEquals(4, bytesRead);
      assertArrayEquals(new byte[]{4, 5, 6, 7}, out,
          "should return bytes starting from seek position, skipping the checksum-aligned preamble");
    }
  }

  /**
   * Defensive test for readFromQueue() which NPE'ed when poll() returns null.
   *
   * This tests a server-error / edge-case scenario: the server sends only a
   * single response whose data ends before the client's seek position, then
   * immediately completes the stream. The while(true) loop in read() skips
   * all bytes in the response (empty buffer), then calls poll() again. poll()
   * finds the queue empty and isDone()==true and returns null.
   *
   * A well-behaved server would never complete the stream without covering the
   * client's position, so this scenario represents a protocol violation rather
   * than normal operation, but it serves to reproduce the NPE exception before
   * fixing the code.
   */
  @Test
  public void testReadFromQueueNpeWhenStreamCompletesWithoutCoveringSeekPosition() throws Exception {
    OzoneClientConfig clientConfig = newStreamReadConfig();
    // Short timeout so the test completes quickly rather than waiting 5 s.
    clientConfig.setStreamReadTimeout(Duration.ofMillis(200));

    BlockID blockID = new BlockID(1L, 13L);
    long length = 8;
    Pipeline pipeline = mockStandalonePipeline();
    ClientCallStreamObserver<ContainerCommandRequestProto> requestObserver =
        mock(ClientCallStreamObserver.class);
    StreamingReadResponse streamingReadResponse = mock(StreamingReadResponse.class);
    when(streamingReadResponse.getRequestObserver()).thenReturn(requestObserver);

    AtomicReference<StreamingReaderSpi> readerRef = new AtomicReference<>();
    XceiverClientGrpc xceiverClient = mock(XceiverClientGrpc.class);
    doAnswer(inv -> {
      StreamingReaderSpi reader = inv.getArgument(1);
      reader.setStreamingReadResponse(streamingReadResponse);
      readerRef.set(reader);
      return null;
    }).when(xceiverClient).initStreamRead(any(BlockID.class), any(), any());

    // Server only sends bytes 0–3 (before seek position 4) then completes —
    // simulating a protocol violation or a truncated/corrupt response.
    doAnswer(inv -> {
      StreamingReaderSpi reader = readerRef.get();
      reader.onNext(buildResponseProto(new byte[]{0, 1, 2, 3}, 0));
      reader.onCompleted();
      return null;
    }).when(xceiverClient).streamRead(any(), any());

    XceiverClientFactory xceiverClientFactory = mock(XceiverClientFactory.class);
    when(xceiverClientFactory.acquireClientForReadData(any(Pipeline.class)))
        .thenReturn(xceiverClient);

    try (StreamBlockInputStream sbis = new StreamBlockInputStream(
        blockID, length, pipeline, null, xceiverClientFactory,
        NO_REFRESH, clientConfig)) {

      sbis.seek(4);

      ByteBuffer buf = ByteBuffer.allocate(4);
      // Before the fixes: threw NullPointerException (Bug 1) or looped forever (Bug 3).
      // After the fixes: returns gracefully with 0 / EOF rather than crashing.
      int bytesRead = sbis.read(buf);
      assertEquals(-1, bytesRead, "should reach EOF when the stream completes before the seek position");
      assertEquals(0, buf.position(), "no bytes should be produced");
    }
  }

  /**
   * When the server delivers multiple responses plus onCompleted() inside a
   * single streamRead() call (all on the same call stack), the first response
   * is consumed correctly, but by the time read() is invoked again for the
   * second chunk, future.isDone() is already true. read() sees isDone() and
   * returns null immediately without checking the queue, so the second (and
   * any further) queued responses are silently dropped.
   */
  @Test
  public void testReadDoesNotDropQueuedItemsWhenFutureIsDoneOnSecondCall() throws Exception {
    OzoneClientConfig clientConfig = newStreamReadConfig();
    BlockID blockID = new BlockID(1L, 11L);
    byte[] firstChunk = {1, 2, 3, 4};
    byte[] secondChunk = {5, 6, 7, 8};
    long length = firstChunk.length + secondChunk.length; // 8 bytes total

    Pipeline pipeline = mockStandalonePipeline();
    ClientCallStreamObserver<ContainerCommandRequestProto> requestObserver =
        mock(ClientCallStreamObserver.class);
    StreamingReadResponse streamingReadResponse = mock(StreamingReadResponse.class);
    when(streamingReadResponse.getRequestObserver()).thenReturn(requestObserver);

    AtomicReference<StreamingReaderSpi> readerRef = new AtomicReference<>();
    XceiverClientGrpc xceiverClient = mock(XceiverClientGrpc.class);
    doAnswer(inv -> {
      StreamingReaderSpi reader = inv.getArgument(1);
      reader.setStreamingReadResponse(streamingReadResponse);
      readerRef.set(reader);
      return null;
    }).when(xceiverClient).initStreamRead(any(BlockID.class), any(), any());

    // Server delivers both 4-byte chunks plus onCompleted() in one synchronous
    // call. After streamRead() returns: queue=[chunk1, chunk2], isDone=true.
    // read() correctly returns chunk1 on the first call, but on the second call
    // it sees isDone()==true and returns null before draining chunk2.
    doAnswer(inv -> {
      StreamingReaderSpi reader = readerRef.get();
      reader.onNext(buildResponseProto(firstChunk, 0));
      reader.onNext(buildResponseProto(secondChunk, firstChunk.length));
      reader.onCompleted(); // future done; both items still in queue
      return null;
    }).when(xceiverClient).streamRead(any(), any());

    XceiverClientFactory xceiverClientFactory = mock(XceiverClientFactory.class);
    when(xceiverClientFactory.acquireClientForReadData(any(Pipeline.class)))
        .thenReturn(xceiverClient);

    try (StreamBlockInputStream sbis = new StreamBlockInputStream(
        blockID, length, pipeline, null, xceiverClientFactory,
        NO_REFRESH, clientConfig)) {
      ByteBuffer buf = ByteBuffer.allocate((int) length);
      // With the bug: read() returns null on the second call (isDone is true),
      // so only 4 bytes are read and buf.position() == 4.
      // After the fix: all 8 bytes are read and buf.position() == 8.
      int bytesRead = sbis.read(buf);
      assertEquals(length, bytesRead, "expected all bytes to be read");
      assertEquals(length, buf.position(), "buffer position should be at end of block");
    }
  }

  @Test
  public void testReadGetsFreshResponseTimeoutAfterStreamReadWait() throws Exception {
    OzoneClientConfig clientConfig = newStreamReadConfig();
    clientConfig.setStreamReadTimeout(Duration.ofMillis(500));
    BlockID blockID = new BlockID(1L, 12L);
    Pipeline pipeline = mockStandalonePipeline();
    ClientCallStreamObserver<ContainerCommandRequestProto> requestObserver =
        mock(ClientCallStreamObserver.class);
    StreamingReadResponse streamingReadResponse = new StreamingReadResponse(
        MockDatanodeDetails.randomDatanodeDetails(), requestObserver);

    XceiverClientGrpc xceiverClient = mock(XceiverClientGrpc.class);
    AtomicReference<StreamingReaderSpi> readerRef = new AtomicReference<>();
    AtomicReference<Thread> responseThreadRef = new AtomicReference<>();
    doAnswer(inv -> {
      StreamingReaderSpi reader = inv.getArgument(1);
      reader.setStreamingReadResponse(streamingReadResponse);
      readerRef.set(reader);
      return null;
    }).when(xceiverClient).initStreamRead(any(BlockID.class), any(), any());
    doAnswer(inv -> {
      Thread.sleep(450);
      Thread responseThread = new Thread(() -> {
        try {
          Thread.sleep(100);
        } catch (InterruptedException ignored) {
          Thread.currentThread().interrupt();
        }
        readerRef.get().onNext(buildResponseProto(new byte[] {1}, 0));
      });
      responseThreadRef.set(responseThread);
      responseThread.start();
      return null;
    }).when(xceiverClient).streamRead(any(), any());

    XceiverClientFactory xceiverClientFactory = mock(XceiverClientFactory.class);
    when(xceiverClientFactory.acquireClientForReadData(any(Pipeline.class)))
        .thenReturn(xceiverClient);

    try (StreamBlockInputStream sbis = new StreamBlockInputStream(
        blockID, 1L, pipeline, null, xceiverClientFactory,
        NO_REFRESH, clientConfig)) {
      ByteBuffer buf = ByteBuffer.allocate(1);
      assertEquals(1, sbis.read(buf));
      responseThreadRef.get().join();
    }
  }

  @Test
  public void testReadWithoutNewRequestGetsFreshTimeoutBudget() throws Exception {
    OzoneClientConfig clientConfig = newStreamReadConfig();
    clientConfig.setStreamReadPreReadSize(10);
    clientConfig.setStreamReadTimeout(Duration.ofMillis(500));
    BlockID blockID = new BlockID(1L, 13L);
    Pipeline pipeline = mockStandalonePipeline();
    ClientCallStreamObserver<ContainerCommandRequestProto> requestObserver =
        mock(ClientCallStreamObserver.class);
    StreamingReadResponse streamingReadResponse = new StreamingReadResponse(
        MockDatanodeDetails.randomDatanodeDetails(), requestObserver);

    AtomicReference<StreamingReaderSpi> readerRef = new AtomicReference<>();
    AtomicInteger streamReads = new AtomicInteger();
    XceiverClientGrpc xceiverClient = mock(XceiverClientGrpc.class);
    doAnswer(inv -> {
      StreamingReaderSpi reader = inv.getArgument(1);
      reader.setStreamingReadResponse(streamingReadResponse);
      readerRef.set(reader);
      return null;
    }).when(xceiverClient).initStreamRead(any(BlockID.class), any(), any());
    doAnswer(inv -> {
      streamReads.incrementAndGet();
      readerRef.get().onNext(buildResponseProto(new byte[] {1}, 0));
      return null;
    }).when(xceiverClient).streamRead(any(), any());

    XceiverClientFactory xceiverClientFactory = mock(XceiverClientFactory.class);
    when(xceiverClientFactory.acquireClientForReadData(any(Pipeline.class)))
        .thenReturn(xceiverClient);

    try (StreamBlockInputStream sbis = new StreamBlockInputStream(
        blockID, 2L, pipeline, null, xceiverClientFactory,
        NO_REFRESH, clientConfig)) {
      ByteBuffer first = ByteBuffer.allocate(1);
      assertEquals(1, sbis.read(first));
      Thread.sleep(600);

      Thread delayedResponse = new Thread(() -> {
        try {
          Thread.sleep(100);
        } catch (InterruptedException ignored) {
          Thread.currentThread().interrupt();
        }
        readerRef.get().onNext(buildResponseProto(new byte[] {2}, 1));
      });
      delayedResponse.start();

      ByteBuffer second = ByteBuffer.allocate(1);
      assertEquals(1, sbis.readFully(second, false));
      delayedResponse.join();
      assertEquals(1, streamReads.get(), "second read should use data from the existing request");
    }
  }

  /**
   * The server may end the stream (onCompleted) after a request has been sent but before any
   * data is delivered, e.g. when the datanode shuts down gracefully. poll() then returns null
   * (stream done, queue drained) while readFromQueue() is waiting for data. The premature end
   * of the stream must surface as EOF, not as a NullPointerException that bypasses
   * handleExceptions() retry handling.
   */
  @Test
  public void testStreamCompletedMidReadWithEmptyQueueSurfacesEof() throws Exception {
    OzoneClientConfig clientConfig = newStreamReadConfig();
    BlockID blockID = new BlockID(1L, 14L);
    Pipeline pipeline = mockStandalonePipeline();
    ClientCallStreamObserver<ContainerCommandRequestProto> requestObserver =
        mock(ClientCallStreamObserver.class);
    StreamingReadResponse streamingReadResponse = mock(StreamingReadResponse.class);
    when(streamingReadResponse.getRequestObserver()).thenReturn(requestObserver);

    AtomicReference<StreamingReaderSpi> readerRef = new AtomicReference<>();
    XceiverClientGrpc xceiverClient = mock(XceiverClientGrpc.class);
    doAnswer(inv -> {
      StreamingReaderSpi reader = inv.getArgument(1);
      reader.setStreamingReadResponse(streamingReadResponse);
      readerRef.set(reader);
      return null;
    }).when(xceiverClient).initStreamRead(any(BlockID.class), any(), any());

    // The server closes the stream without delivering any of the requested data, so the
    // reader ends up polling a drained queue with the future already completed.
    doAnswer(inv -> {
      readerRef.get().onCompleted();
      return null;
    }).when(xceiverClient).streamRead(any(), any());

    XceiverClientFactory xceiverClientFactory = mock(XceiverClientFactory.class);
    when(xceiverClientFactory.acquireClientForReadData(any(Pipeline.class)))
        .thenReturn(xceiverClient);

    try (StreamBlockInputStream sbis = new StreamBlockInputStream(
        blockID, 4L, pipeline, null, xceiverClientFactory,
        NO_REFRESH, clientConfig)) {
      ByteBuffer buf = ByteBuffer.allocate(4);
      int bytesRead = assertDoesNotThrow(() -> sbis.read(buf),
          "premature stream completion must not throw NullPointerException");
      assertEquals(-1, bytesRead, "premature stream completion should surface as EOF");
      assertEquals(0, buf.position());
    }
  }

  /**
   * A truncated payload fails checksum verification in onNext(). The failure handling must
   * record the real failure via setFailed() and propagate it, even though the payload is
   * shorter than the 10-byte hex preview included in the warning log.
   */
  @Test
  public void testChecksumFailureOnShortPayloadSurfacesRealError() throws Exception {
    OzoneClientConfig clientConfig = newStreamReadConfig();
    clientConfig.setChecksumVerify(true);
    BlockID blockID = new BlockID(1L, 15L);
    byte[] data = {1, 2, 3, 4};
    Pipeline pipeline = mockStandalonePipeline();
    ClientCallStreamObserver<ContainerCommandRequestProto> requestObserver =
        mock(ClientCallStreamObserver.class);
    StreamingReadResponse streamingReadResponse = mock(StreamingReadResponse.class);
    when(streamingReadResponse.getRequestObserver()).thenReturn(requestObserver);

    AtomicReference<StreamingReaderSpi> readerRef = new AtomicReference<>();
    XceiverClientGrpc xceiverClient = mock(XceiverClientGrpc.class);
    doAnswer(inv -> {
      StreamingReaderSpi reader = inv.getArgument(1);
      reader.setStreamingReadResponse(streamingReadResponse);
      readerRef.set(reader);
      return null;
    }).when(xceiverClient).initStreamRead(any(BlockID.class), any(), any());

    // Deliver a payload shorter than 10 bytes whose checksum does not match the data.
    doAnswer(inv -> {
      readerRef.get().onNext(buildCorruptResponseProto(data, 0));
      return null;
    }).when(xceiverClient).streamRead(any(), any());

    XceiverClientFactory xceiverClientFactory = mock(XceiverClientFactory.class);
    when(xceiverClientFactory.acquireClientForReadData(any(Pipeline.class)))
        .thenReturn(xceiverClient);

    try (StreamBlockInputStream sbis = new StreamBlockInputStream(
        blockID, data.length, pipeline, null, xceiverClientFactory,
        NO_REFRESH, clientConfig)) {
      ByteBuffer buf = ByteBuffer.allocate(data.length);
      IOException thrown = assertThrows(IOException.class, () -> sbis.read(buf),
          "checksum failure should surface as an IOException");
      assertThat(thrown).hasRootCauseInstanceOf(OzoneChecksumException.class);
    }
    verify(requestObserver, times(1)).onError(any(OzoneChecksumException.class));
  }

  /**
   * onNext() may fail before XceiverClientGrpc has registered the StreamingReadResponse, so
   * getResponse() is still null while the failure is reported. The real failure must still be
   * recorded and surfaced to the reader.
   */
  @Test
  public void testChecksumFailureBeforeResponseRegistered() throws Exception {
    OzoneClientConfig clientConfig = newStreamReadConfig();
    clientConfig.setChecksumVerify(true);
    BlockID blockID = new BlockID(1L, 16L);
    byte[] data = {1, 2, 3};
    Pipeline pipeline = mockStandalonePipeline();
    ClientCallStreamObserver<ContainerCommandRequestProto> requestObserver =
        mock(ClientCallStreamObserver.class);
    StreamingReadResponse streamingReadResponse = mock(StreamingReadResponse.class);
    when(streamingReadResponse.getRequestObserver()).thenReturn(requestObserver);

    XceiverClientGrpc xceiverClient = mock(XceiverClientGrpc.class);
    doAnswer(inv -> {
      StreamingReaderSpi reader = inv.getArgument(1);
      // The corrupt response arrives before setStreamingReadResponse() has been called.
      reader.onNext(buildCorruptResponseProto(data, 0));
      reader.setStreamingReadResponse(streamingReadResponse);
      return null;
    }).when(xceiverClient).initStreamRead(any(BlockID.class), any(), any());

    XceiverClientFactory xceiverClientFactory = mock(XceiverClientFactory.class);
    when(xceiverClientFactory.acquireClientForReadData(any(Pipeline.class)))
        .thenReturn(xceiverClient);

    try (StreamBlockInputStream sbis = new StreamBlockInputStream(
        blockID, data.length, pipeline, null, xceiverClientFactory,
        NO_REFRESH, clientConfig)) {
      ByteBuffer buf = ByteBuffer.allocate(data.length);
      IOException thrown = assertThrows(IOException.class, () -> sbis.read(buf),
          "checksum failure should surface as an IOException");
      assertThat(thrown).hasRootCauseInstanceOf(OzoneChecksumException.class);
    }
    verify(requestObserver, never()).onError(any());
  }

  /**
   * A streamed error response (e.g. CONTAINER_NOT_FOUND) must fail the read immediately
   * instead of stalling until the stream read timeout expires.
   */
  @Test
  public void testFailFastOnErrorResponseProto() throws Exception {
    OzoneClientConfig clientConfig = newStreamReadConfig();
    clientConfig.setMaxReadRetryCount(0);
    BlockID blockID = new BlockID(1L, 20L);
    ClientCallStreamObserver<ContainerCommandRequestProto> requestObserver =
        mock(ClientCallStreamObserver.class);

    XceiverClientGrpc xceiverClient = mockCapturingStreamingReadClient(requestObserver,
        reader -> reader.onNext(ContainerCommandResponseProto.newBuilder()
            .setCmdType(Type.ReadBlock)
            .setResult(ContainerProtos.Result.CONTAINER_NOT_FOUND)
            .setMessage("Container not found")
            .build()));
    XceiverClientFactory xceiverClientFactory = mock(XceiverClientFactory.class);
    when(xceiverClientFactory.acquireClientForReadData(any(Pipeline.class)))
        .thenReturn(xceiverClient);

    try (StreamBlockInputStream sbis = new StreamBlockInputStream(
        blockID, 1024L, mockStandalonePipeline(), null, xceiverClientFactory,
        NO_REFRESH, clientConfig)) {

      ByteBuffer buf = ByteBuffer.allocate(1024);
      final long startTime = System.nanoTime();
      IOException thrown = assertThrows(IOException.class, () -> sbis.read(buf),
          "an error response proto should surface as an IOException");
      final long elapsedMillis = Duration.ofNanos(System.nanoTime() - startTime).toMillis();

      assertThat(hasCause(thrown, StorageContainerException.class)).isTrue();
      assertThat(hasCause(thrown, TimeoutIOException.class)).isFalse();
      assertThat(elapsedMillis).isLessThan(sbis.getReadTimeout().toMillis());
      verify(requestObserver, times(1)).onError(any(StorageContainerException.class));
    }
  }

  /**
   * A server-sent gRPC status other than CANCELLED (here OUT_OF_RANGE) must fail the
   * pending read promptly rather than waiting for the stream read timeout.
   */
  @Test
  public void testFailFastOnGrpcOutOfRangeStatus() throws Exception {
    OzoneClientConfig clientConfig = newStreamReadConfig();
    clientConfig.setMaxReadRetryCount(0);
    BlockID blockID = new BlockID(1L, 21L);
    ClientCallStreamObserver<ContainerCommandRequestProto> requestObserver =
        mock(ClientCallStreamObserver.class);

    XceiverClientGrpc xceiverClient = mockCapturingStreamingReadClient(requestObserver,
        reader -> reader.onError(Status.OUT_OF_RANGE.asRuntimeException()));
    XceiverClientFactory xceiverClientFactory = mock(XceiverClientFactory.class);
    when(xceiverClientFactory.acquireClientForReadData(any(Pipeline.class)))
        .thenReturn(xceiverClient);

    try (StreamBlockInputStream sbis = new StreamBlockInputStream(
        blockID, 1024L, mockStandalonePipeline(), null, xceiverClientFactory,
        NO_REFRESH, clientConfig)) {

      ByteBuffer buf = ByteBuffer.allocate(1024);
      final long startTime = System.nanoTime();
      IOException thrown = assertThrows(IOException.class, () -> sbis.read(buf),
          "a gRPC OUT_OF_RANGE should surface as an IOException");
      final long elapsedMillis = Duration.ofNanos(System.nanoTime() - startTime).toMillis();

      assertThat(hasCause(thrown, StatusRuntimeException.class)).isTrue();
      assertThat(hasCause(thrown, TimeoutIOException.class)).isFalse();
      assertThat(elapsedMillis).isLessThan(sbis.getReadTimeout().toMillis());
    }
  }

  /**
   * Mocks a streaming read client which captures the StreamingReaderSpi during
   * initStreamRead and drives the given callback when a ReadBlock request is sent.
   */
  private XceiverClientGrpc mockCapturingStreamingReadClient(
      ClientCallStreamObserver<ContainerCommandRequestProto> requestObserver,
      Consumer<StreamingReaderSpi> onStreamRead) throws Exception {
    StreamingReadResponse streamingReadResponse = mock(StreamingReadResponse.class);
    when(streamingReadResponse.getRequestObserver()).thenReturn(requestObserver);

    AtomicReference<StreamingReaderSpi> readerRef = new AtomicReference<>();
    XceiverClientGrpc xceiverClient = mock(XceiverClientGrpc.class);
    doAnswer(inv -> {
      StreamingReaderSpi reader = inv.getArgument(1);
      reader.setStreamingReadResponse(streamingReadResponse);
      readerRef.set(reader);
      return null;
    }).when(xceiverClient).initStreamRead(any(BlockID.class), any(), any());

    doAnswer(inv -> {
      onStreamRead.accept(readerRef.get());
      return null;
    }).when(xceiverClient).streamRead(any(), any());

    return xceiverClient;
  }

  private static boolean hasCause(Throwable throwable, Class<? extends Throwable> type) {
    for (Throwable t = throwable; t != null; t = t.getCause()) {
      if (type.isInstance(t)) {
        return true;
      }
    }
    return false;
  }

  private ReadBlockResponseProto buildReadBlockResponse(byte[] data) {
    return ReadBlockResponseProto.newBuilder()
        .setOffset(0)
        .setData(ByteString.copyFrom(data))
        .setChecksumData(ChecksumData.newBuilder()
            .setType(ContainerProtos.ChecksumType.NONE)
            .setBytesPerChecksum(data.length)
            .build())
        .build();
  }

  private ContainerCommandResponseProto buildResponseProto(byte[] data, long offset) {
    return ContainerCommandResponseProto.newBuilder()
        .setCmdType(Type.ReadBlock)
        .setResult(ContainerProtos.Result.SUCCESS)
        .setReadBlock(ReadBlockResponseProto.newBuilder()
            .setOffset(offset)
            .setData(ByteString.copyFrom(data))
            .setChecksumData(ChecksumData.newBuilder()
                .setType(ContainerProtos.ChecksumType.NONE)
                .setBytesPerChecksum(data.length)
                .build())
            .build())
        .build();
  }

  private ContainerCommandResponseProto buildCorruptResponseProto(byte[] data, long offset) {
    return ContainerCommandResponseProto.newBuilder()
        .setCmdType(Type.ReadBlock)
        .setResult(ContainerProtos.Result.SUCCESS)
        .setReadBlock(ReadBlockResponseProto.newBuilder()
            .setOffset(offset)
            .setData(ByteString.copyFrom(data))
            .setChecksumData(ChecksumData.newBuilder()
                .setType(ContainerProtos.ChecksumType.CRC32)
                .setBytesPerChecksum(data.length)
                .addChecksums(ByteString.copyFrom(new byte[4]))
                .build())
            .build())
        .build();
  }

  // ------------------------------------------------------------------------------------------------------
  // Positioned reads: read(long, ByteBuffer) runs on its own one-shot streaming read.
  // ------------------------------------------------------------------------------------------------------

  /**
   * A pread asks for exactly the requested range: one ReadBlock request with offset == the pread offset and
   * length clamped to the end of the block, never inflated by the pre-read size.
   */
  @Test
  public void testPreadIssuesSingleExactRequest() throws Exception {
    OzoneClientConfig clientConfig = newStreamReadConfig();
    clientConfig.setStreamReadPreReadSize(64L << 20);
    byte[] data = sequentialBytes(32);
    RecordingStreamClient client = new RecordingStreamClient(
        Collections.singletonList(MockDatanodeDetails.randomDatanodeDetails()),
        (reader, request, datanode) -> serveRange(reader, request, data));
    XceiverClientFactory xceiverClientFactory = mockFactory(client);

    try (StreamBlockInputStream sbis = new StreamBlockInputStream(
        new BlockID(1L, 30L), data.length, mockStandalonePipeline(), null, xceiverClientFactory,
        NO_REFRESH, clientConfig)) {

      assertPread(sbis, data, 8, 10);
      assertEquals(1, client.requests.size());
      assertEquals(8, client.requests.get(0).getReadBlock().getOffset());
      assertEquals(10, client.requests.get(0).getReadBlock().getLength());

      // A pread which runs off the end of the block is clamped to the remaining bytes.
      ByteBuffer tail = ByteBuffer.allocate(16);
      assertEquals(8, sbis.read(24, tail));
      assertArrayEquals(Arrays.copyOfRange(data, 24, 32), Arrays.copyOf(tail.array(), 8));
      assertEquals(2, client.requests.size());
      assertEquals(24, client.requests.get(1).getReadBlock().getOffset());
      assertEquals(8, client.requests.get(1).getReadBlock().getLength());

      assertEquals(0, sbis.getPos());
    }
  }

  @Test
  public void testPreadHalfClosesOnceOnSuccess() throws Exception {
    assertPreadHalfClosesExactlyOnce(newStreamReadConfig(), sequentialBytes(8),
        (reader, request, datanode) -> serveRange(reader, request, sequentialBytes(8)), true);
  }

  @Test
  public void testPreadHalfClosesOnceOnChecksumFailure() throws Exception {
    OzoneClientConfig clientConfig = newStreamReadConfig();
    clientConfig.setChecksumVerify(true);
    clientConfig.setMaxReadRetryCount(0);
    byte[] data = sequentialBytes(8);
    assertPreadHalfClosesExactlyOnce(clientConfig, data,
        (reader, request, datanode) -> reader.onNext(buildCorruptResponseProto(data, 0)), false);
  }

  @Test
  public void testPreadHalfClosesOnceOnErrorResponse() throws Exception {
    OzoneClientConfig clientConfig = newStreamReadConfig();
    clientConfig.setMaxReadRetryCount(0);
    assertPreadHalfClosesExactlyOnce(clientConfig, sequentialBytes(8),
        (reader, request, datanode) -> reader.onNext(ContainerCommandResponseProto.newBuilder()
            .setCmdType(Type.ReadBlock)
            .setResult(ContainerProtos.Result.CONTAINER_NOT_FOUND)
            .setMessage("Container not found")
            .build()), false);
  }

  @Test
  public void testPreadHalfClosesOnceOnTimeout() throws Exception {
    OzoneClientConfig clientConfig = newStreamReadConfig();
    clientConfig.setMaxReadRetryCount(0);
    clientConfig.setStreamReadTimeout(Duration.ofMillis(200));
    assertPreadHalfClosesExactlyOnce(clientConfig, sequentialBytes(8),
        (reader, request, datanode) -> { }, false);
  }

  /**
   * Preads issued before, between and after sequential reads leave the cursor, the sequential reader and its
   * request stream untouched, and every byte they return is correct.
   */
  @Test
  public void testPreadDoesNotDisturbSequentialReads() throws Exception {
    OzoneClientConfig clientConfig = newStreamReadConfig();
    byte[] data = sequentialBytes(64);
    RecordingStreamClient client = new RecordingStreamClient(
        Collections.singletonList(MockDatanodeDetails.randomDatanodeDetails()),
        (reader, request, datanode) -> serveRange(reader, request, data));
    XceiverClientFactory xceiverClientFactory = mockFactory(client);

    try (StreamBlockInputStream sbis = new StreamBlockInputStream(
        new BlockID(1L, 31L), data.length, mockStandalonePipeline(), null, xceiverClientFactory,
        NO_REFRESH, clientConfig)) {

      // Before any sequential read: no cursor, no sequential reader.
      assertPread(sbis, data, 40, 8);
      assertEquals(0, sbis.getPos());
      assertNull(sequentialReader(sbis));

      byte[] head = new byte[16];
      assertEquals(16, sbis.read(head, 0, 16));
      assertArrayEquals(Arrays.copyOfRange(data, 0, 16), head);
      assertEquals(16, sbis.getPos());

      final StreamingReaderSpi sequentialReader = sequentialReader(sbis);
      assertNotNull(sequentialReader);
      // TestStreamReadDatanodeFailover reaches the serving datanode through this exact declared method.
      assertNotNull(sequentialReader.getClass().getDeclaredMethod("getResponse"));
      final int sequentialRequests = client.requests.size();

      // Between sequential reads.
      assertPread(sbis, data, 0, 8);
      assertEquals(16, sbis.getPos());
      assertSame(sequentialReader, sequentialReader(sbis));

      sbis.seek(32);
      assertPread(sbis, data, 20, 12);
      assertEquals(32, sbis.getPos());
      assertSame(sequentialReader, sequentialReader(sbis));

      byte[] tail = new byte[16];
      assertEquals(16, sbis.read(tail, 0, 16));
      assertArrayEquals(Arrays.copyOfRange(data, 32, 48), tail);
      assertEquals(48, sbis.getPos());

      // The preads sent their own requests, the sequential reader only its own.
      assertEquals(sequentialRequests + 3, client.requests.size());
      verify(client.observerOf(sequentialReader), never()).onCompleted();

      sbis.unbuffer();
      assertPread(sbis, data, 56, 8);
      assertEquals(48, sbis.getPos());
      // unbuffer(), not the preads, half-closed the sequential stream.
      verify(client.observerOf(sequentialReader), times(1)).onCompleted();
    }
  }

  /**
   * Concurrent preads each get their own streaming read: all of them reach initStreamRead before any response
   * is delivered, and each one returns its own range.
   */
  @Test
  public void testConcurrentPreadsUseIndependentStreams() throws Exception {
    final int threads = 8;
    final int chunk = 16;
    OzoneClientConfig clientConfig = newStreamReadConfig();
    byte[] data = sequentialBytes(threads * chunk);
    CountDownLatch started = new CountDownLatch(threads);
    CountDownLatch release = new CountDownLatch(1);
    RecordingStreamClient client = new RecordingStreamClient(
        Collections.singletonList(MockDatanodeDetails.randomDatanodeDetails()),
        (reader, request, datanode) -> {
          started.countDown();
          assertTrue(release.await(30, TimeUnit.SECONDS), "responses should be released");
          serveRange(reader, request, data);
        });
    XceiverClientFactory xceiverClientFactory = mockFactory(client);

    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try (StreamBlockInputStream sbis = new StreamBlockInputStream(
        new BlockID(1L, 32L), data.length, mockStandalonePipeline(), null, xceiverClientFactory,
        NO_REFRESH, clientConfig)) {

      List<Future<byte[]>> results = new ArrayList<>();
      for (int i = 0; i < threads; i++) {
        final long offset = (long) i * chunk;
        results.add(pool.submit(() -> {
          ByteBuffer buf = ByteBuffer.allocate(chunk);
          assertEquals(chunk, sbis.read(offset, buf));
          return buf.array();
        }));
      }

      assertTrue(started.await(30, TimeUnit.SECONDS), "every pread should reach its own streaming read");
      assertEquals(threads, client.readers.size(), "one reader per pread");
      assertEquals(threads, new HashSet<>(client.readers).size(), "readers must be distinct");
      release.countDown();

      for (int i = 0; i < threads; i++) {
        assertArrayEquals(Arrays.copyOfRange(data, i * chunk, (i + 1) * chunk),
            results.get(i).get(30, TimeUnit.SECONDS));
      }
      assertEquals(0, sbis.getPos());
    } finally {
      pool.shutdownNow();
    }
    verify(client.client, times(threads)).completeStreamRead();
  }

  /**
   * A connectivity failure excludes the failing datanode from the retry, refreshes the block location once and
   * completes on the next datanode without disturbing the sequential retry counter.
   */
  @Test
  public void testPreadExcludesFailedDatanodeAndRetries() throws Exception {
    OzoneClientConfig clientConfig = newStreamReadConfig();
    clientConfig.setReadRetryInterval(0);
    byte[] data = sequentialBytes(16);
    DatanodeDetails first = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails second = MockDatanodeDetails.randomDatanodeDetails();
    RecordingStreamClient client = new RecordingStreamClient(Arrays.asList(first, second),
        (reader, request, datanode) -> {
          if (datanode.getID().equals(first.getID())) {
            reader.onError(new StatusRuntimeException(Status.UNAVAILABLE));
          } else {
            serveRange(reader, request, data);
          }
        });
    XceiverClientFactory xceiverClientFactory = mockFactory(client);
    AtomicInteger refreshes = new AtomicInteger();
    Function<BlockID, BlockLocationInfo> refreshFunction = b -> {
      refreshes.incrementAndGet();
      return null;
    };

    try (StreamBlockInputStream sbis = new StreamBlockInputStream(
        new BlockID(1L, 33L), data.length, mockStandalonePipeline(), null, xceiverClientFactory,
        refreshFunction, clientConfig)) {

      assertPread(sbis, data, 0, data.length);
      assertEquals(1, refreshes.get(), "the block location should be refreshed once");
      assertEquals(2, client.excludedSets.size());
      assertThat(client.excludedSets.get(0)).isEmpty();
      assertThat(client.excludedSets.get(1)).contains(first.getID());
      assertEquals(0, sequentialRetries(sbis), "a pread must not touch the sequential retry counter");

      // The sequential path still works, on the datanode which is left.
      byte[] out = new byte[data.length];
      assertEquals(data.length, sbis.read(out, 0, data.length));
      assertArrayEquals(data, out);
      assertEquals(0, sequentialRetries(sbis));
    }
  }

  /**
   * The datanode reports a read beyond the end of the block with a gRPC OUT_OF_RANGE status, which must surface
   * as EOFException; a pread starting at or after the block length short-circuits to EOF.
   */
  @Test
  public void testPreadOutOfRangeSurfacesEof() throws Exception {
    OzoneClientConfig clientConfig = newStreamReadConfig();
    byte[] data = sequentialBytes(8);
    RecordingStreamClient client = new RecordingStreamClient(
        Collections.singletonList(MockDatanodeDetails.randomDatanodeDetails()),
        (reader, request, datanode) -> reader.onError(Status.OUT_OF_RANGE.asRuntimeException()));
    XceiverClientFactory xceiverClientFactory = mockFactory(client);

    try (StreamBlockInputStream sbis = new StreamBlockInputStream(
        new BlockID(1L, 34L), data.length, mockStandalonePipeline(), null, xceiverClientFactory,
        NO_REFRESH, clientConfig)) {

      assertThrows(EOFException.class, () -> sbis.readFully(0, ByteBuffer.allocate(4)),
          "an OUT_OF_RANGE response should surface as EOFException");
      assertEquals(1, client.readers.size(), "OUT_OF_RANGE must not be retried");

      assertEquals(-1, sbis.read(data.length, ByteBuffer.allocate(4)));
      assertEquals(-1, sbis.read(data.length + 10L, ByteBuffer.allocate(4)));
      assertEquals(-1, sbis.read(-1L, ByteBuffer.allocate(4)));
      assertEquals(0, sbis.read(0, ByteBuffer.allocate(0)));
      assertEquals(1, client.readers.size(), "out of range preads must not open a stream");
    }
  }

  @Test
  public void testHasPreadByteBufferCapability() throws Exception {
    try (StreamBlockInputStream sbis = new StreamBlockInputStream(
        new BlockID(1L, 35L), 1024L, mockStandalonePipeline(), null, mock(XceiverClientFactory.class),
        NO_REFRESH, newStreamReadConfig())) {
      assertTrue(sbis.hasCapability("in:preadbytebuffer"));
      assertTrue(sbis.hasCapability("in:readbytebuffer"));
      assertFalse(sbis.hasCapability("in:something-else"));
    }
  }

  /**
   * Runs a single pread against {@code handler} and asserts that its request stream was half-closed and its
   * streaming permit released exactly once, whether it succeeded or failed.
   */
  private void assertPreadHalfClosesExactlyOnce(OzoneClientConfig clientConfig, byte[] data,
      StreamReadHandler handler, boolean expectSuccess) throws Exception {
    RecordingStreamClient client = new RecordingStreamClient(
        Collections.singletonList(MockDatanodeDetails.randomDatanodeDetails()), handler);
    XceiverClientFactory xceiverClientFactory = mockFactory(client);

    try (StreamBlockInputStream sbis = new StreamBlockInputStream(
        new BlockID(1L, 36L), data.length, mockStandalonePipeline(), null, xceiverClientFactory,
        NO_REFRESH, clientConfig)) {
      ByteBuffer buf = ByteBuffer.allocate(data.length);
      if (expectSuccess) {
        assertEquals(data.length, sbis.read(0, buf));
        assertArrayEquals(data, buf.array());
      } else {
        assertThrows(IOException.class, () -> sbis.read(0, buf));
      }
    }

    assertEquals(1, client.observers.size(), "exactly one streaming read per pread");
    verify(client.observers.get(0), times(1)).onCompleted();
    verify(client.client, times(1)).completeStreamRead();
    verify(xceiverClientFactory, times(1)).releaseClientForReadData(client.client, false);
  }

  private void assertPread(StreamBlockInputStream sbis, byte[] data, int blockOffset, int length)
      throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(length);
    assertEquals(length, sbis.read(blockOffset, buf));
    assertArrayEquals(Arrays.copyOfRange(data, blockOffset, blockOffset + length), buf.array());
  }

  private void serveRange(StreamingReaderSpi reader, ContainerCommandRequestProto request, byte[] blockData) {
    final int offset = Math.toIntExact(request.getReadBlock().getOffset());
    final int length = Math.toIntExact(request.getReadBlock().getLength());
    reader.onNext(buildResponseProto(Arrays.copyOfRange(blockData, offset, offset + length), offset));
  }

  private XceiverClientFactory mockFactory(RecordingStreamClient client) throws IOException {
    XceiverClientFactory xceiverClientFactory = mock(XceiverClientFactory.class);
    when(xceiverClientFactory.acquireClientForReadData(any(Pipeline.class))).thenReturn(client.client);
    return xceiverClientFactory;
  }

  private static byte[] sequentialBytes(int length) {
    byte[] data = new byte[length];
    for (int i = 0; i < length; i++) {
      data[i] = (byte) i;
    }
    return data;
  }

  /** The sequential reader is read by reflection, as TestStreamReadDatanodeFailover does. */
  private static StreamingReaderSpi sequentialReader(StreamBlockInputStream sbis) throws Exception {
    final Field field = StreamBlockInputStream.class.getDeclaredField("streamingReader");
    field.setAccessible(true);
    return (StreamingReaderSpi) field.get(sbis);
  }

  private static int sequentialRetries(StreamBlockInputStream sbis) throws Exception {
    final Field field = StreamBlockInputStream.class.getDeclaredField("retries");
    field.setAccessible(true);
    return field.getInt(sbis);
  }

  /** Drives the reader when a ReadBlock request reaches the datanode which serves the stream. */
  @FunctionalInterface
  private interface StreamReadHandler {
    void handle(StreamingReaderSpi reader, ContainerCommandRequestProto request, DatanodeDetails datanode)
        throws Exception;
  }

  /**
   * A streaming read client which gives every initStreamRead its own request observer and
   * StreamingReadResponse, and records the reader, the excluded-datanode snapshot and every request sent.
   */
  private static final class RecordingStreamClient {
    private final XceiverClientGrpc client = mock(XceiverClientGrpc.class);
    private final List<StreamingReaderSpi> readers = Collections.synchronizedList(new ArrayList<>());
    private final List<Set<DatanodeID>> excludedSets = Collections.synchronizedList(new ArrayList<>());
    private final List<ContainerCommandRequestProto> requests = Collections.synchronizedList(new ArrayList<>());
    private final List<ClientCallStreamObserver<ContainerCommandRequestProto>> observers =
        Collections.synchronizedList(new ArrayList<>());
    private final Map<StreamingReaderSpi, ClientCallStreamObserver<ContainerCommandRequestProto>> readerObservers =
        new ConcurrentHashMap<>();
    private final Map<StreamingReadResponse, StreamingReaderSpi> readerByResponse = new ConcurrentHashMap<>();

    private RecordingStreamClient(List<DatanodeDetails> datanodes, StreamReadHandler handler) throws Exception {
      doAnswer(inv -> {
        final StreamingReaderSpi reader = inv.getArgument(1);
        final Set<DatanodeID> excluded = inv.getArgument(2);
        final DatanodeDetails datanode = datanodes.stream()
            .filter(dn -> !excluded.contains(dn.getID()))
            .findFirst()
            .orElseThrow(() -> new IOException("All datanodes are excluded"));
        final ClientCallStreamObserver<ContainerCommandRequestProto> observer =
            mock(ClientCallStreamObserver.class);
        final StreamingReadResponse response = new StreamingReadResponse(datanode, observer);
        excludedSets.add(new HashSet<>(excluded));
        observers.add(observer);
        readers.add(reader);
        readerObservers.put(reader, observer);
        readerByResponse.put(response, reader);
        reader.setStreamingReadResponse(response);
        return null;
      }).when(client).initStreamRead(any(BlockID.class), any(), any());

      doAnswer(inv -> {
        final ContainerCommandRequestProto request = inv.getArgument(0);
        final StreamingReadResponse response = inv.getArgument(1);
        requests.add(request);
        handler.handle(readerByResponse.get(response), request, response.getDatanodeDetails());
        return null;
      }).when(client).streamRead(any(), any());
    }

    private ClientCallStreamObserver<ContainerCommandRequestProto> observerOf(StreamingReaderSpi reader) {
      return readerObservers.get(reader);
    }
  }
}
