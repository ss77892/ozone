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

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_NOT_FOUND;
import static org.apache.hadoop.hdds.scm.storage.TestChunkInputStream.generateRandomData;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.primitives.Bytes;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.StatusException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.stubbing.OngoingStubbing;
import org.slf4j.event.Level;

/**
 * Tests for {@link BlockInputStream}'s functionality.
 */
public class TestBlockInputStream {

  private static final int CHUNK_SIZE = 100;

  private Checksum checksum;
  private BlockInputStream blockStream;
  private byte[] blockData;
  private int blockSize;
  private List<ChunkInfo> chunks;
  private Map<String, byte[]> chunkDataMap;

  private Function<BlockID, BlockLocationInfo> refreshFunction;

  private OzoneConfiguration conf = new OzoneConfiguration();

  @BeforeEach
  @SuppressWarnings("unchecked")
  public void setup() throws Exception {
    refreshFunction = mock(Function.class);
    BlockID blockID = new BlockID(new ContainerBlockID(1, 1));
    checksum = new Checksum(ChecksumType.NONE, CHUNK_SIZE);
    createChunkList(5);
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(false);

    Pipeline pipeline = MockPipeline.createSingleNodePipeline();
    blockStream = new DummyBlockInputStream(blockID, blockSize, pipeline, null,
        null, refreshFunction, chunks, chunkDataMap, clientConfig);
  }

  /**
   * Create a mock list of chunks. The first n-1 chunks of length CHUNK_SIZE
   * and the last chunk with length CHUNK_SIZE/2.
   */
  private void createChunkList(int numChunks)
      throws Exception {

    chunks = new ArrayList<>(numChunks);
    chunkDataMap = new HashMap<>();
    blockData = new byte[0];
    int i, chunkLen;
    byte[] byteData;
    String chunkName;

    for (i = 0; i < numChunks; i++) {
      chunkName = "chunk-" + i;
      chunkLen = CHUNK_SIZE;
      if (i == numChunks - 1) {
        chunkLen = CHUNK_SIZE / 2;
      }
      byteData = generateRandomData(chunkLen);
      ChunkInfo chunkInfo = ChunkInfo.newBuilder()
          .setChunkName(chunkName)
          .setOffset(0)
          .setLen(chunkLen)
          .setChecksumData(checksum.computeChecksum(
              byteData, 0, chunkLen).getProtoBufMessage())
          .build();

      chunkDataMap.put(chunkName, byteData);
      chunks.add(chunkInfo);

      blockSize += chunkLen;
      blockData = Bytes.concat(blockData, byteData);
    }
  }

  private void seekAndVerify(int pos) throws Exception {
    blockStream.seek(pos);
    assertEquals(pos, blockStream.getPos(),
        "Current position of buffer does not match with the sought position");
  }

  /**
   * Match readData with the chunkData byte-wise.
   * @param readData Data read through ChunkInputStream
   * @param inputDataStartIndex first index (inclusive) in chunkData to compare
   *                            with read data
   * @param length the number of bytes of data to match starting from
   *               inputDataStartIndex
   */
  private void matchWithInputData(byte[] readData, int inputDataStartIndex,
      int length) {
    for (int i = inputDataStartIndex; i < inputDataStartIndex + length; i++) {
      assertEquals(blockData[i], readData[i - inputDataStartIndex]);
    }
  }

  @Test
  public void testSeek() throws Exception {
    // Seek to position 0
    int pos = 0;
    seekAndVerify(pos);
    assertEquals(0, blockStream.getChunkIndex(), "ChunkIndex is incorrect");

    // Before BlockInputStream is initialized (initialization happens during
    // read operation), seek should update the BlockInputStream#blockPosition
    pos = CHUNK_SIZE;
    seekAndVerify(pos);
    assertEquals(0, blockStream.getChunkIndex(), "ChunkIndex is incorrect");
    assertEquals(pos, blockStream.getBlockPosition());

    // Initialize the BlockInputStream. After initialization, the chunkIndex
    // should be updated to correspond to the sought position.
    blockStream.initialize();
    assertEquals(1, blockStream.getChunkIndex(), "ChunkIndex is incorrect");

    pos = (CHUNK_SIZE * 4) + 5;
    seekAndVerify(pos);
    assertEquals(4, blockStream.getChunkIndex(), "ChunkIndex is incorrect");
    pos = blockSize + 10;

    int finalPos = pos;
    assertThrows(EOFException.class, () -> seekAndVerify(finalPos));

    // Seek to random positions between 0 and the block size.
    for (int i = 0; i < 10; i++) {
      pos = RandomUtils.secure().randomInt(0, blockSize);
      seekAndVerify(pos);
    }
  }

  @Test
  public void testRead() throws Exception {
    // read 200 bytes of data starting from position 50. Chunk0 contains
    // indices 0 to 99, chunk1 from 100 to 199 and chunk3 from 200 to 299. So
    // the read should result in 3 ChunkInputStream reads
    seekAndVerify(50);
    byte[] b = new byte[200];
    int bytesRead = blockStream.read(b, 0, 200);
    assertEquals(200, bytesRead, "Expected to read 200 bytes");
    matchWithInputData(b, 50, 200);

    // The new position of the blockInputStream should be the last index read
    // + 1.
    assertEquals(250, blockStream.getPos());
    assertEquals(2, blockStream.getChunkIndex());
  }

  @Test
  public void testReadWithByteBuffer() throws Exception {
    // read 200 bytes of data starting from position 50. Chunk0 contains
    // indices 0 to 99, chunk1 from 100 to 199 and chunk3 from 200 to 299. So
    // the read should result in 3 ChunkInputStream reads
    seekAndVerify(50);
    ByteBuffer buffer = ByteBuffer.allocate(200);
    blockStream.read(buffer);
    matchWithInputData(buffer.array(), 50, 200);

    // The new position of the blockInputStream should be the last index read
    // + 1.
    assertEquals(250, blockStream.getPos());
    assertEquals(2, blockStream.getChunkIndex());
  }

  @Test
  public void testReadWithDirectByteBuffer() throws Exception {
    // read 200 bytes of data starting from position 50. Chunk0 contains
    // indices 0 to 99, chunk1 from 100 to 199 and chunk3 from 200 to 299. So
    // the read should result in 3 ChunkInputStream reads
    seekAndVerify(50);
    ByteBuffer buffer = ByteBuffer.allocateDirect(200);
    blockStream.read(buffer);
    for (int i = 50; i < 50 + 200; i++) {
      assertEquals(blockData[i], buffer.get(i - 50));
    }

    // The new position of the blockInputStream should be the last index read
    // + 1.
    assertEquals(250, blockStream.getPos());
    assertEquals(2, blockStream.getChunkIndex());
  }

  @Test
  public void testSeekAndRead() throws Exception {
    // Seek to a position and read data
    seekAndVerify(50);
    byte[] b1 = new byte[100];
    int bytesRead1 = blockStream.read(b1, 0, 100);
    assertEquals(100, bytesRead1, "Expected to read 100 bytes");
    matchWithInputData(b1, 50, 100);

    // Next read should start from the position of the last read + 1 i.e. 100
    byte[] b2 = new byte[100];
    int bytesRead2 = blockStream.read(b2, 0, 100);
    assertEquals(100, bytesRead2, "Expected to read 100 bytes");
    matchWithInputData(b2, 150, 100);
  }

  @Test
  public void testRefreshPipelineFunction() throws Exception {
    LogCapturer logCapturer = LogCapturer.captureLogs(BlockExtendedInputStream.class);
    GenericTestUtils.setLogLevel(BlockExtendedInputStream.class, Level.DEBUG);
    BlockID blockID = new BlockID(new ContainerBlockID(1, 1));
    AtomicBoolean isRefreshed = new AtomicBoolean();
    createChunkList(5);
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(false);

    try (BlockInputStream blockInputStreamWithRetry =
             new DummyBlockInputStreamWithRetry(blockID, blockSize,
                 MockPipeline.createSingleNodePipeline(), null,
                 null, chunks, chunkDataMap, isRefreshed, null,
                 clientConfig)) {
      assertFalse(isRefreshed.get());
      seekAndVerify(50);
      byte[] b = new byte[200];
      int bytesRead = blockInputStreamWithRetry.read(b, 0, 200);
      assertEquals(200, bytesRead, "Expected to read 200 bytes");
      assertThat(logCapturer.getOutput()).contains("Retry read after");
      assertTrue(isRefreshed.get());
    }
  }

  @ParameterizedTest
  @MethodSource("exceptionsTriggersRefresh")
  void refreshesPipelineOnReadFailure(IOException ex) throws Exception {
    // GIVEN
    Pipeline pipeline = MockPipeline.createSingleNodePipeline();
    BlockLocationInfo blockLocationInfo = mock(BlockLocationInfo.class);
    when(blockLocationInfo.getPipeline()).thenReturn(pipeline);
    Pipeline newPipeline = MockPipeline.createSingleNodePipeline();
    BlockLocationInfo newBlockLocationInfo = mock(BlockLocationInfo.class);

    testRefreshesPipelineOnReadFailure(ex, blockLocationInfo,
        id -> newBlockLocationInfo);

    when(newBlockLocationInfo.getPipeline()).thenReturn(newPipeline);
    testRefreshesPipelineOnReadFailure(ex, blockLocationInfo,
        id -> blockLocationInfo);

    when(newBlockLocationInfo.getPipeline()).thenReturn(null);
    testRefreshesPipelineOnReadFailure(ex, blockLocationInfo,
        id -> newBlockLocationInfo);
  }

  private void testRefreshesPipelineOnReadFailure(IOException ex,
      BlockLocationInfo blockLocationInfo,
      Function<BlockID, BlockLocationInfo> refreshPipelineFunction)
      throws Exception {

    BlockID blockID = new BlockID(new ContainerBlockID(1, 1));

    final int len = 200;
    final ChunkInputStream stream = throwingChunkInputStream(ex, len, true);

    when(this.refreshFunction.apply(any()))
        .thenAnswer(inv -> refreshPipelineFunction.apply(blockID));

    try (BlockInputStream subject = createSubject(blockID,
        blockLocationInfo.getPipeline(), stream)) {
      subject.initialize();

      // WHEN
      byte[] b = new byte[len];
      int bytesRead = subject.read(b, 0, len);

      // THEN
      assertEquals(len, bytesRead);
      verify(this.refreshFunction).apply(blockID);
    } finally {
      reset(this.refreshFunction);
    }
  }

  private static Stream<Arguments> exceptionsNotTriggerRefresh() {
    return Stream.of(
        Arguments.of(new SCMSecurityException("Security problem")),
        Arguments.of(new OzoneChecksumException("checksum missing")),
        Arguments.of(new IOException("Some random exception."))
    );
  }

  private static ChunkInputStream throwingChunkInputStream(IOException ex,
      int len, boolean succeedOnRetry) throws IOException {
    final ChunkInputStream stream = mock(ChunkInputStream.class);
    OngoingStubbing<Integer> stubbing =
        when(stream.read(any(), anyInt(), anyInt()))
            .thenThrow(ex);
    if (succeedOnRetry) {
      stubbing.thenReturn(len);
    }
    when(stream.getRemaining())
        .thenReturn((long) len);
    return stream;
  }

  private BlockInputStream createSubject(BlockID blockID, Pipeline pipeline,
      ChunkInputStream stream) throws IOException {
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(false);
    return new DummyBlockInputStream(blockID, blockSize, pipeline, null,
        null, refreshFunction, chunks, null, clientConfig) {
      @Override
      protected ChunkInputStream createChunkInputStream(ChunkInfo chunkInfo) {
        return stream;
      }
    };
  }

  @ParameterizedTest
  @MethodSource("exceptionsNotTriggerRefresh")
  public void testReadNotRetriedOnOtherException(IOException ex)
      throws Exception {
    // GIVEN
    BlockID blockID = new BlockID(new ContainerBlockID(1, 1));
    Pipeline pipeline = MockPipeline.createSingleNodePipeline();

    final int len = ThreadLocalRandom.current().nextInt(100, 300);
    final ChunkInputStream stream = throwingChunkInputStream(ex, len, false);

    try (BlockInputStream subject = createSubject(blockID, pipeline, stream)) {
      subject.initialize();

      // WHEN
      assertThrows(ex.getClass(),
          () -> {
            byte[] buffer = new byte[len];
            int bytesRead = subject.read(buffer, 0, len);
            // This line should never be reached due to the exception
            assertEquals(len, bytesRead);
          });

      // THEN
      verify(refreshFunction, never()).apply(blockID);
    }
  }

  @ParameterizedTest
  @MethodSource("exceptionsTriggersRefresh")
  public void testRefreshOnReadFailureAfterUnbuffer(IOException ex)
      throws Exception {
    // GIVEN
    BlockID blockID = new BlockID(new ContainerBlockID(1, 1));
    Pipeline pipeline = MockPipeline.createSingleNodePipeline();
    Pipeline newPipeline = MockPipeline.createSingleNodePipeline();
    XceiverClientFactory clientFactory = mock(XceiverClientFactory.class);
    XceiverClientSpi client = mock(XceiverClientSpi.class);
    BlockLocationInfo blockLocationInfo = mock(BlockLocationInfo.class);
    when(clientFactory.acquireClientForReadData(pipeline))
        .thenReturn(client);

    final int len = 200;
    final ChunkInputStream stream = throwingChunkInputStream(ex, len, true);

    when(refreshFunction.apply(blockID))
        .thenReturn(blockLocationInfo);
    when(blockLocationInfo.getPipeline()).thenReturn(newPipeline);

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(false);
    BlockInputStream subject = new BlockInputStream(
        new BlockLocationInfo(new BlockLocationInfo.Builder().setBlockID(blockID).setLength(blockSize)),
        pipeline, null, clientFactory, refreshFunction,
        clientConfig) {
      @Override
      protected ChunkInputStream createChunkInputStream(ChunkInfo chunkInfo) {
        return stream;
      }

      @Override
      protected ContainerProtos.BlockData getBlockDataUsingClient() throws IOException {
        BlockID blockID = getBlockID();
        ContainerProtos.DatanodeBlockID datanodeBlockID = blockID.getDatanodeBlockIDProtobuf();
        return ContainerProtos.BlockData.newBuilder().addAllChunks(chunks).setBlockID(datanodeBlockID).build();
      }
    };

    try {
      subject.initialize();
      subject.unbuffer();

      // WHEN
      byte[] b = new byte[len];
      int bytesRead = subject.read(b, 0, len);

      // THEN
      assertEquals(len, bytesRead);
      verify(refreshFunction).apply(blockID);
      verify(clientFactory).acquireClientForReadData(pipeline);
      verify(clientFactory).releaseClientForReadData(client, false);
    } finally {
      subject.close();
    }
  }

  private static Stream<Arguments> exceptionsTriggersRefresh() {
    return Stream.of(
        Arguments.of(new StorageContainerException(CONTAINER_NOT_FOUND)),
        Arguments.of(new IOException(new ExecutionException(
            new StatusException(Status.UNAVAILABLE))))
    );
  }

  /**
   * Rebuild the chunk list with a checksum stored for every {@code bytesPerChecksum} bytes, so that the
   * range a positioned read has to align to is smaller than a chunk.
   */
  private void createChunkListWithChecksumEvery(int bytesPerChecksum) throws Exception {
    checksum = new Checksum(ChecksumType.CRC32, bytesPerChecksum);
    blockSize = 0;
    createChunkList(5);
  }

  private byte[] blockDataRange(int from, int to) {
    return Arrays.copyOfRange(blockData, from, to);
  }

  private static List<Long> chunkStreamPositions(BlockInputStream stream) {
    List<Long> positions = new ArrayList<>();
    for (ChunkInputStream chunkStream : stream.getChunkStreams()) {
      positions.add(chunkStream.getPos());
    }
    return positions;
  }

  @Test
  public void testPositionedReadAcrossChunkBoundary() throws Exception {
    // Checksums are stored for every 20 bytes, so each ReadChunk must cover the checksum boundaries
    // around the requested range and nothing more.
    createChunkListWithChecksumEvery(20);

    try (RecordingBlockInputStream subject = createRecordingStream(true)) {
      ByteBuffer buffer = ByteBuffer.allocate(100);
      assertEquals(100, subject.read(150, buffer));
      assertArrayEquals(blockDataRange(150, 250), buffer.array());

      // chunk-1 holds block bytes 100-199, chunk-2 holds 200-299, so exactly two chunks are covered.
      List<ChunkInfo> requests = subject.getReadChunkRequests();
      assertEquals(2, requests.size());
      // 50-99 of chunk-1: aligned down to 40 and up to the end of the covering checksum boundary (100).
      assertEquals("chunk-1", requests.get(0).getChunkName());
      assertEquals(40, requests.get(0).getOffset());
      assertEquals(60, requests.get(0).getLen());
      // 0-49 of chunk-2: aligned up to the end of the covering checksum boundary (60).
      assertEquals("chunk-2", requests.get(1).getChunkName());
      assertEquals(0, requests.get(1).getOffset());
      assertEquals(60, requests.get(1).getLen());
    }
  }

  @Test
  public void testPositionedReadGetsBlockDataOnce() throws Exception {
    try (RecordingBlockInputStream subject = createRecordingStream(false)) {
      for (int i = 0; i < 100; i++) {
        ByteBuffer buffer = ByteBuffer.allocate(10);
        assertEquals(10, subject.read(i, buffer));
        assertArrayEquals(blockDataRange(i, i + 10), buffer.array());
      }
      assertEquals(1, subject.getBlockDataCount());
    }
  }

  @Test
  public void testConcurrentPositionedReads() throws Exception {
    final int threadCount = 8;
    final int readLen = 50;
    final CountDownLatch insideReadChunk = new CountDownLatch(threadCount);
    final CountDownLatch release = new CountDownLatch(1);

    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    try (RecordingBlockInputStream subject = createRecordingStream(false, () -> {
      insideReadChunk.countDown();
      try {
        assertTrue(release.await(60, TimeUnit.SECONDS), "readChunk was never released");
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      }
    }, 0)) {
      subject.initialize();

      List<Future<byte[]>> futures = new ArrayList<>(threadCount);
      for (int i = 0; i < threadCount; i++) {
        final int offset = i * readLen;
        futures.add(executor.submit(() -> {
          ByteBuffer buffer = ByteBuffer.allocate(readLen);
          assertEquals(readLen, subject.read(offset, buffer));
          return buffer.array();
        }));
      }

      // Every positioned read must be inside its own ReadChunk before any of them is allowed to finish.
      assertTrue(insideReadChunk.await(60, TimeUnit.SECONDS),
          "Positioned reads did not run concurrently");
      release.countDown();

      for (int i = 0; i < threadCount; i++) {
        assertArrayEquals(blockDataRange(i * readLen, (i + 1) * readLen), futures.get(i).get());
      }
      assertEquals(threadCount, subject.getReadChunkRequests().size());
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void testPositionedReadDoesNotDisturbSequentialRead() throws Exception {
    seekAndVerify(50);
    byte[] sequential = new byte[120];
    assertEquals(120, blockStream.read(sequential, 0, 120));
    matchWithInputData(sequential, 50, 120);
    assertEquals(170, blockStream.getPos());
    assertEquals(1, blockStream.getChunkIndex());
    List<Long> positionsBefore = chunkStreamPositions(blockStream);

    ByteBuffer buffer = ByteBuffer.allocate(150);
    assertEquals(150, blockStream.read(280, buffer));
    assertArrayEquals(blockDataRange(280, 430), buffer.array());
    byte[] preadBytes = new byte[40];
    assertEquals(40, blockStream.read(10, preadBytes, 0, 40));
    matchWithInputData(preadBytes, 10, 40);

    // Neither the cursor nor the buffers of the sequential chunk streams have moved.
    assertEquals(170, blockStream.getPos());
    assertEquals(1, blockStream.getChunkIndex());
    assertEquals(positionsBefore, chunkStreamPositions(blockStream));

    // The sequential read continues where it left off.
    byte[] next = new byte[80];
    assertEquals(80, blockStream.read(next, 0, 80));
    matchWithInputData(next, 170, 80);
    assertEquals(250, blockStream.getPos());

    // ... and so does a sequential read after seek and unbuffer, with positioned reads in between.
    seekAndVerify(300);
    blockStream.unbuffer();
    ByteBuffer afterUnbuffer = ByteBuffer.allocate(60);
    assertEquals(60, blockStream.read(0, afterUnbuffer));
    assertArrayEquals(blockDataRange(0, 60), afterUnbuffer.array());
    assertEquals(300, blockStream.getPos());
    assertEquals(3, blockStream.getChunkIndex());
    byte[] afterSeek = new byte[100];
    assertEquals(100, blockStream.read(afterSeek, 0, 100));
    matchWithInputData(afterSeek, 300, 100);
  }

  @Test
  public void testPreadByteBufferCapability() throws Exception {
    // No client factory (closed or unit test stream): nothing forces the reads to be serialized.
    assertTrue(blockStream.hasCapability(StreamCapabilities.PREADBYTEBUFFER));

    XceiverClientFactory clientFactory = mock(XceiverClientFactory.class);
    when(clientFactory.isShortCircuitEnabled()).thenReturn(false);
    try (BlockInputStream subject = createSubjectWithFactory(clientFactory)) {
      assertTrue(subject.hasCapability(StreamCapabilities.PREADBYTEBUFFER));
      subject.seek(30);
      ByteBuffer buffer = ByteBuffer.allocate(150);
      assertEquals(150, subject.read(120, buffer));
      assertArrayEquals(blockDataRange(120, 270), buffer.array());
      assertEquals(30, subject.getPos());
    }

    XceiverClientFactory shortCircuitFactory = mock(XceiverClientFactory.class);
    when(shortCircuitFactory.isShortCircuitEnabled()).thenReturn(true);
    try (BlockInputStream subject = createSubjectWithFactory(shortCircuitFactory)) {
      // A single FileInputStream is shared by all the chunks of the block, so the positioned read falls
      // back to the serialized default which moves the cursor and restores it.
      assertFalse(subject.hasCapability(StreamCapabilities.PREADBYTEBUFFER));
      assertTrue(subject.hasCapability(StreamCapabilities.READBYTEBUFFER));
      subject.seek(30);
      ByteBuffer buffer = ByteBuffer.allocate(150);
      assertEquals(150, subject.read(120, buffer));
      assertArrayEquals(blockDataRange(120, 270), buffer.array());
      assertEquals(30, subject.getPos());
    }
  }

  @Test
  public void testPositionedReadAtEndOfBlock() throws Exception {
    assertEquals(-1, blockStream.read(blockSize, ByteBuffer.allocate(10)));
    assertEquals(-1, blockStream.read(blockSize + 10, ByteBuffer.allocate(10)));
    assertEquals(-1, blockStream.read(-1, ByteBuffer.allocate(10)));
    assertEquals(0, blockStream.read(0, ByteBuffer.allocate(0)));

    // A read which goes past the end of the block is clamped to the block length.
    ByteBuffer tail = ByteBuffer.allocate(100);
    assertEquals(50, blockStream.read(blockSize - 50, tail));
    assertArrayEquals(blockDataRange(blockSize - 50, blockSize), Arrays.copyOf(tail.array(), 50));

    byte[] b = new byte[120];
    assertEquals(120, blockStream.read(60, b, 0, 120));
    matchWithInputData(b, 60, 120);
    blockStream.readFully(200, b);
    matchWithInputData(b, 200, 120);
    assertThrows(EOFException.class, () -> blockStream.readFully(blockSize - 10, new byte[20]));
    assertEquals(-1, blockStream.read(blockSize, b, 0, 10));

    assertEquals(0, blockStream.getPos());
  }

  @Test
  public void testPositionedReadRefreshesPipelineOnFailure() throws Exception {
    BlockID blockID = new BlockID(new ContainerBlockID(1, 1));
    BlockLocationInfo blockLocationInfo = mock(BlockLocationInfo.class);
    when(blockLocationInfo.getPipeline())
        .thenReturn(MockPipeline.createSingleNodePipeline());
    when(refreshFunction.apply(any())).thenReturn(blockLocationInfo);

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(false);
    clientConfig.setReadRetryInterval(0);

    try (RecordingBlockInputStream subject =
             new RecordingBlockInputStream(null, false, null, 1, clientConfig)) {
      ByteBuffer buffer = ByteBuffer.allocate(80);
      assertEquals(80, subject.read(30, buffer));
      assertArrayEquals(blockDataRange(30, 110), buffer.array());
      verify(refreshFunction, times(1)).apply(blockID);
      // The first ReadChunk failed and was retried on a fresh ephemeral stream.
      assertEquals(3, subject.getReadChunkRequests().size());

      // The sequential read is not affected by the failed positioned read.
      byte[] b = new byte[200];
      assertEquals(200, subject.read(b, 0, 200));
      matchWithInputData(b, 0, 200);
      assertEquals(200, subject.getPos());
      assertEquals(2, subject.getChunkIndex());
    }
  }

  @Test
  public void testPreadDoesNotWaitForParkedSequentialRead() throws Exception {
    final CountDownLatch parked = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(false);

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try (SequentialGatingBlockInputStream subject =
             new SequentialGatingBlockInputStream(parked, release, clientConfig)) {
      subject.gateNewChunkStreams(true);
      subject.initialize();
      subject.gateNewChunkStreams(false);

      byte[] sequential = new byte[50];
      Future<Integer> sequentialRead =
          executor.submit(() -> subject.read(sequential, 0, sequential.length));
      // The sequential read now holds the monitor of the stream inside readWithStrategy and is parked
      // inside the readChunk of the chunk stream created by initialize().
      assertTrue(parked.await(10, TimeUnit.SECONDS), "The sequential read never reached readChunk");

      Future<byte[]> positionedRead = executor.submit(() -> {
        ByteBuffer buffer = ByteBuffer.allocate(50);
        assertEquals(50, subject.read(100, buffer));
        return buffer.array();
      });
      assertArrayEquals(blockDataRange(100, 150), positionedRead.get(10, TimeUnit.SECONDS));

      // The positioned read issued its own ReadChunk on an ephemeral, ungated chunk stream while the
      // sequential read was still parked in its own.
      List<ChunkInfo> requests = subject.getReadChunkRequests();
      assertEquals(2, requests.size());
      assertEquals("chunk-0", requests.get(0).getChunkName());
      assertEquals("chunk-1", requests.get(1).getChunkName());

      release.countDown();
      assertEquals(50, sequentialRead.get(10, TimeUnit.SECONDS).intValue());
      matchWithInputData(sequential, 0, 50);
      assertEquals(50, subject.getPos());
      assertEquals(1, subject.getBlockDataCount());
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void testPreadAfterCloseThrows() throws Exception {
    XceiverClientFactory clientFactory = mock(XceiverClientFactory.class);
    when(clientFactory.isShortCircuitEnabled()).thenReturn(false);
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(false);

    // DummyBlockInputStream cannot express this: its checkOpen() is a no-op whatever the factory is.
    BlockInputStream subject = new BlockInputStream(
        new BlockLocationInfo(new BlockLocationInfo.Builder()
            .setBlockID(new BlockID(new ContainerBlockID(1, 1))).setLength(blockSize)),
        MockPipeline.createSingleNodePipeline(), null, clientFactory, refreshFunction, clientConfig) {
      @Override
      protected ContainerProtos.BlockData getBlockData() {
        return ContainerProtos.BlockData.newBuilder().addAllChunks(chunks)
            .setBlockID(getBlockID().getDatanodeBlockIDProtobuf()).build();
      }

      @Override
      protected ChunkInputStream createChunkInputStream(ChunkInfo chunkInfo) {
        return new DummyChunkInputStream(chunkInfo, null, null, false,
            chunkDataMap.get(chunkInfo.getChunkName()).clone(), null);
      }
    };

    ByteBuffer buffer = ByteBuffer.allocate(80);
    assertEquals(80, subject.read(30, buffer));
    assertArrayEquals(blockDataRange(30, 110), buffer.array());

    subject.close();
    IOException ex = assertThrows(IOException.class, () -> subject.read(30, ByteBuffer.allocate(80)));
    assertThat(ex.getMessage()).contains("has been closed");
  }

  private BlockInputStream createSubjectWithFactory(XceiverClientFactory clientFactory)
      throws IOException {
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(false);
    return new DummyBlockInputStream(new BlockID(new ContainerBlockID(1, 1)), blockSize,
        MockPipeline.createSingleNodePipeline(), null, clientFactory, refreshFunction, chunks,
        chunkDataMap, clientConfig);
  }

  private RecordingBlockInputStream createRecordingStream(boolean chunkVerifyChecksum)
      throws IOException {
    return createRecordingStream(chunkVerifyChecksum, null, 0);
  }

  private RecordingBlockInputStream createRecordingStream(boolean chunkVerifyChecksum,
      Runnable readChunkGate, int failures) throws IOException {
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(false);
    clientConfig.setReadRetryInterval(0);
    return new RecordingBlockInputStream(null, chunkVerifyChecksum, readChunkGate, failures,
        clientConfig);
  }

  /**
   * A DummyBlockInputStream which records the ChunkInfo of every ReadChunk issued by the streams it
   * creates, counts the GetBlock calls, and can gate or fail the reads.
   */
  private final class RecordingBlockInputStream extends DummyBlockInputStream {

    private final List<ChunkInfo> readChunkRequests =
        Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger getBlockDataCount = new AtomicInteger();
    private final AtomicInteger failuresToInject;
    private final boolean chunkVerifyChecksum;
    private final Runnable readChunkGate;

    RecordingBlockInputStream(XceiverClientFactory clientFactory, boolean chunkVerifyChecksum,
        Runnable readChunkGate, int failures, OzoneClientConfig config) throws IOException {
      super(new BlockID(new ContainerBlockID(1, 1)), blockSize,
          MockPipeline.createSingleNodePipeline(), null, clientFactory, refreshFunction, chunks,
          chunkDataMap, config);
      this.chunkVerifyChecksum = chunkVerifyChecksum;
      this.readChunkGate = readChunkGate;
      this.failuresToInject = new AtomicInteger(failures);
    }

    @Override
    protected ContainerProtos.BlockData getBlockData() throws IOException {
      getBlockDataCount.incrementAndGet();
      return super.getBlockData();
    }

    @Override
    protected ChunkInputStream createChunkInputStream(ChunkInfo chunkInfo) {
      return new DummyChunkInputStream(chunkInfo, null, null, chunkVerifyChecksum,
          chunkDataMap.get(chunkInfo.getChunkName()), null) {
        @Override
        protected ByteBuffer[] readChunk(ChunkInfo readChunkInfo) throws IOException {
          readChunkRequests.add(readChunkInfo);
          if (failuresToInject.getAndUpdate(count -> count > 0 ? count - 1 : 0) > 0) {
            throw new StorageContainerException("Simulated read failure", CONTAINER_NOT_FOUND);
          }
          if (readChunkGate != null) {
            readChunkGate.run();
          }
          return super.readChunk(readChunkInfo);
        }
      };
    }

    List<ChunkInfo> getReadChunkRequests() {
      return readChunkRequests;
    }

    int getBlockDataCount() {
      return getBlockDataCount.get();
    }
  }

  /**
   * A DummyBlockInputStream whose chunk streams created while {@link #gateNewChunkStreams(boolean)} is on
   * park inside readChunk until they are released, so that a sequential read can be held inside a chunk
   * stream of the block while the ephemeral chunk streams of the positioned reads run freely.
   */
  private final class SequentialGatingBlockInputStream extends DummyBlockInputStream {

    private final List<ChunkInfo> readChunkRequests =
        Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger getBlockDataCount = new AtomicInteger();
    private final AtomicBoolean gateNewStreams = new AtomicBoolean();
    private final CountDownLatch parked;
    private final CountDownLatch release;

    SequentialGatingBlockInputStream(CountDownLatch parked, CountDownLatch release,
        OzoneClientConfig config) throws IOException {
      super(new BlockID(new ContainerBlockID(1, 1)), blockSize,
          MockPipeline.createSingleNodePipeline(), null, null, refreshFunction, chunks,
          chunkDataMap, config);
      this.parked = parked;
      this.release = release;
    }

    void gateNewChunkStreams(boolean gate) {
      gateNewStreams.set(gate);
    }

    @Override
    protected ContainerProtos.BlockData getBlockData() throws IOException {
      getBlockDataCount.incrementAndGet();
      return super.getBlockData();
    }

    @Override
    protected ChunkInputStream createChunkInputStream(ChunkInfo chunkInfo) {
      final boolean gated = gateNewStreams.get();
      return new DummyChunkInputStream(chunkInfo, null, null, false,
          chunkDataMap.get(chunkInfo.getChunkName()), null) {
        @Override
        protected ByteBuffer[] readChunk(ChunkInfo readChunkInfo) throws IOException {
          readChunkRequests.add(readChunkInfo);
          if (gated) {
            parked.countDown();
            try {
              assertTrue(release.await(60, TimeUnit.SECONDS), "readChunk was never released");
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new IllegalStateException(e);
            }
          }
          return super.readChunk(readChunkInfo);
        }
      };
    }

    List<ChunkInfo> getReadChunkRequests() {
      return readChunkRequests;
    }

    int getBlockDataCount() {
      return getBlockDataCount.get();
    }
  }
}
