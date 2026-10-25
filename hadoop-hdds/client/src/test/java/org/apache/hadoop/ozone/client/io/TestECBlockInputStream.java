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

package org.apache.hadoop.ozone.client.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.storage.BlockExtendedInputStream;
import org.apache.hadoop.hdds.scm.storage.BlockLocationInfo;
import org.apache.hadoop.hdds.scm.storage.ByteReaderStrategy;
import org.apache.hadoop.hdds.scm.storage.PositionedReadTestHelper;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for ECBlockInputStream, run with cells read one at a time and in parallel.
 */
@ParameterizedClass
@ValueSource(booleans = {false, true})
public class TestECBlockInputStream {

  @Parameter
  private boolean parallelRead;

  private static final int ONEMB = 1024 * 1024;

  private ECReplicationConfig repConfig;
  private TestBlockInputStreamFactory streamFactory;
  private OzoneConfiguration conf = new OzoneConfiguration();
  // counts the tasks handed to the parallel read executor
  private final AtomicInteger parallelReadTasks = new AtomicInteger();
  private ExecutorService parallelReadExecutor;

  @BeforeEach
  public void setup() {
    repConfig = new ECReplicationConfig(3, 2,
        ECReplicationConfig.EcCodec.RS, ONEMB);
    streamFactory = new TestBlockInputStreamFactory();
    // same shape as the client's pool: no queue, and the caller runs the task when it is saturated
    parallelReadExecutor = new ThreadPoolExecutor(0, 30, 60, TimeUnit.SECONDS, new SynchronousQueue<>(),
        new ThreadPoolExecutor.CallerRunsPolicy()) {
      @Override
      public void execute(Runnable command) {
        parallelReadTasks.incrementAndGet();
        super.execute(command);
      }
    };
  }

  @AfterEach
  public void teardown() {
    parallelReadExecutor.shutdownNow();
  }

  @Test
  public void testSufficientLocations() {
    // EC-3-2, 5MB block, so all 3 data locations are needed
    BlockLocationInfo keyInfo = ECStreamTestUtil
        .createKeyInfo(repConfig, 5, 5 * ONEMB);
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, null, null, new TestBlockInputStreamFactory(),
        clientConfig)) {
      assertTrue(ecb.hasSufficientLocations());
    }

    // EC-3-2, very large block, so all 3 data locations are needed
    keyInfo = ECStreamTestUtil.createKeyInfo(repConfig, 5, 5000L * ONEMB);
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, null, null, new TestBlockInputStreamFactory(),
        clientConfig)) {
      assertTrue(ecb.hasSufficientLocations());
    }

    Map<DatanodeDetails, Integer> dnMap = new HashMap<>();

    // EC-3-2, 1 byte short of 1MB with 1 location
    dnMap.put(MockDatanodeDetails.randomDatanodeDetails(), 1);
    keyInfo = ECStreamTestUtil.createKeyInfo(repConfig, ONEMB - 1, dnMap);
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, null, null, new TestBlockInputStreamFactory(),
        clientConfig)) {
      assertTrue(ecb.hasSufficientLocations());
    }

    // EC-3-2, 5MB blocks, only 2 locations passed so we do not have sufficient
    // locations.
    dnMap.clear();
    dnMap.put(MockDatanodeDetails.randomDatanodeDetails(), 1);
    keyInfo = ECStreamTestUtil.createKeyInfo(repConfig, 5 * ONEMB, dnMap);
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, null, null, new TestBlockInputStreamFactory(),
        clientConfig)) {
      assertFalse(ecb.hasSufficientLocations());
    }

    // EC-3-2, 5MB blocks, only 1 data and 2 parity locations present. For now
    // this will fail as we don't support reconstruction reads yet.
    dnMap.clear();
    dnMap.put(MockDatanodeDetails.randomDatanodeDetails(), 1);
    dnMap.put(MockDatanodeDetails.randomDatanodeDetails(), 4);
    dnMap.put(MockDatanodeDetails.randomDatanodeDetails(), 5);
    keyInfo = ECStreamTestUtil.createKeyInfo(repConfig, 5 * ONEMB, dnMap);
    try (ECBlockInputStream ecb = new ECBlockInputStream(repConfig,
        keyInfo, null, null, new TestBlockInputStreamFactory(),
        clientConfig)) {
      assertFalse(ecb.hasSufficientLocations());
    }
  }

  @Test
  public void testCorrectBlockSizePassedToBlockStreamLessThanCell()
      throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(3 * ONEMB);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, ONEMB - 100);

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    try (ECBlockInputStream ecb = createStream(keyInfo, streamFactory, clientConfig)) {
      ecb.read(buf);
      // We expect only 1 block stream and it should have a length passed of
      // ONEMB - 100.
      List<TestBlockInputStream> streams = streamFactory.getBlockStreams();
      assertEquals(ONEMB - 100, streams.get(0).getLength());
    }
  }

  @Test
  public void testCorrectBlockSizePassedToBlockStreamTwoCells()
      throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(3 * ONEMB);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, ONEMB + 100);

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    try (ECBlockInputStream ecb = createStream(keyInfo, streamFactory, clientConfig)) {
      ecb.read(buf);
      List<TestBlockInputStream> streams = streamFactory.getBlockStreams();
      assertEquals(ONEMB, streams.get(0).getLength());
      assertEquals(100, streams.get(1).getLength());
    }
  }

  @Test
  public void testCorrectBlockSizePassedToBlockStreamThreeCells()
      throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(3 * ONEMB);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, 2 * ONEMB + 100);

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    try (ECBlockInputStream ecb = createStream(keyInfo, streamFactory, clientConfig)) {
      ecb.read(buf);
      List<TestBlockInputStream> streams = streamFactory.getBlockStreams();
      assertEquals(ONEMB, streams.get(0).getLength());
      assertEquals(ONEMB, streams.get(1).getLength());
      assertEquals(100, streams.get(2).getLength());
    }
  }

  @Test
  public void testCorrectBlockSizePassedToBlockStreamThreeFullAndPartialStripe()
      throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(3 * ONEMB);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, 10 * ONEMB + 100);

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    try (ECBlockInputStream ecb = createStream(keyInfo, streamFactory, clientConfig)) {
      ecb.read(buf);
      List<TestBlockInputStream> streams = streamFactory.getBlockStreams();
      assertEquals(4 * ONEMB, streams.get(0).getLength());
      assertEquals(3 * ONEMB + 100, streams.get(1).getLength());
      assertEquals(3 * ONEMB, streams.get(2).getLength());
    }
  }

  @Test
  public void testCorrectBlockSizePassedToBlockStreamSingleFullCell()
      throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(3 * ONEMB);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, ONEMB);

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    try (ECBlockInputStream ecb = createStream(keyInfo, streamFactory, clientConfig)) {
      ecb.read(buf);
      List<TestBlockInputStream> streams = streamFactory.getBlockStreams();
      assertEquals(ONEMB, streams.get(0).getLength());
    }
  }

  @Test
  public void testCorrectBlockSizePassedToBlockStreamSeveralFullCells()
      throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(3 * ONEMB);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, 9 * ONEMB);

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    try (ECBlockInputStream ecb = createStream(keyInfo, streamFactory, clientConfig)) {
      ecb.read(buf);
      List<TestBlockInputStream> streams = streamFactory.getBlockStreams();
      assertEquals(3 * ONEMB, streams.get(0).getLength());
      assertEquals(3 * ONEMB, streams.get(1).getLength());
      assertEquals(3 * ONEMB, streams.get(2).getLength());
    }
  }

  @Test
  public void testSimpleRead() throws IOException {
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, 5 * ONEMB);
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    try (ECBlockInputStream ecb = createStream(keyInfo, streamFactory, clientConfig)) {

      ByteBuffer buf = ByteBuffer.allocate(100);

      int read = ecb.read(buf);
      assertEquals(100, read);
      validateBufferContents(buf, 0, 100, (byte) 0);
      assertEquals(100, ecb.getPos());
    }
    for (TestBlockInputStream s : streamFactory.getBlockStreams()) {
      assertTrue(s.isClosed());
    }
  }

  /**
   * This test is to ensure we can read a small key of 1 chunk or less when only
   * the first replica index is available.
   */
  @Test
  public void testSimpleReadUnderOneChunk() throws IOException {
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 1, ONEMB);
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    try (ECBlockInputStream ecb = createStream(keyInfo, streamFactory, clientConfig)) {

      ByteBuffer buf = ByteBuffer.allocate(100);

      int read = ecb.read(buf);
      assertEquals(100, read);
      validateBufferContents(buf, 0, 100, (byte) 0);
      assertEquals(100, ecb.getPos());
    }
    for (TestBlockInputStream s : streamFactory.getBlockStreams()) {
      assertTrue(s.isClosed());
    }
  }

  @Test
  public void testReadPastEOF() throws IOException {
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, 50);
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    try (ECBlockInputStream ecb = createStream(keyInfo, streamFactory, clientConfig)) {

      ByteBuffer buf = ByteBuffer.allocate(100);

      int read = ecb.read(buf);
      assertEquals(50, read);
      read = ecb.read(buf);
      assertEquals(read, -1);
    }
  }

  @Test
  public void testReadCrossingMultipleECChunkBounds() throws IOException {
    // EC-3-2, 5MB block, so all 3 data locations are needed
    repConfig = new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS,
        100);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, 5 * ONEMB);
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    try (ECBlockInputStream ecb = createStream(keyInfo, streamFactory, clientConfig)) {

      // EC Chunk size is 100 and 3-2. Create a byte buffer to read 3.5 chunks,
      // so 350
      ByteBuffer buf = ByteBuffer.allocate(350);
      int read = ecb.read(buf);
      assertEquals(350, read);

      validateBufferContents(buf, 0, 100, (byte) 0);
      validateBufferContents(buf, 100, 200, (byte) 1);
      validateBufferContents(buf, 200, 300, (byte) 2);
      validateBufferContents(buf, 300, 350, (byte) 0);

      buf.clear();
      read = ecb.read(buf);
      assertEquals(350, read);

      validateBufferContents(buf, 0, 50, (byte) 0);
      validateBufferContents(buf, 50, 150, (byte) 1);
      validateBufferContents(buf, 150, 250, (byte) 2);
      validateBufferContents(buf, 250, 350, (byte) 0);

    }
    assertEquals(parallelRead, parallelReadTasks.get() > 0);
    for (TestBlockInputStream s : streamFactory.getBlockStreams()) {
      assertTrue(s.isClosed());
    }
  }

  @Test
  public void testSeekPastBlockLength() throws IOException {
    repConfig = new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS,
        ONEMB);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, 100);
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    try (ECBlockInputStream ecb = createStream(keyInfo, streamFactory, clientConfig)) {
      assertThrows(EOFException.class, () -> ecb.seek(1000));
    }
  }

  @Test
  public void testSeekToLength() throws IOException {
    repConfig = new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS,
        ONEMB);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, 100);
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    try (ECBlockInputStream ecb = createStream(keyInfo, streamFactory, clientConfig)) {
      // When seek more than the length, should throw EOFException.
      assertThrows(EOFException.class, () -> ecb.seek(101));
    }
  }

  @Test
  public void testSeekToLengthZeroLengthBlock() throws IOException {
    repConfig = new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS,
        ONEMB);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, 0);
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    try (ECBlockInputStream ecb = createStream(keyInfo, streamFactory, clientConfig)) {
      ecb.seek(0);
      assertEquals(0, ecb.getPos());
      assertEquals(0, ecb.getRemaining());
    }
  }

  @Test
  public void testSeekToValidPosition() throws IOException {
    repConfig = new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS,
        ONEMB);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, 5 * ONEMB);
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    try (ECBlockInputStream ecb = createStream(keyInfo, streamFactory, clientConfig)) {
      ecb.seek(ONEMB - 1);
      assertEquals(ONEMB - 1, ecb.getPos());
      assertEquals(ONEMB * 4 + 1, ecb.getRemaining());
      // First read should read the last byte of the first chunk
      assertEquals(0, ecb.read());
      assertEquals(ONEMB,
          streamFactory.getBlockStreams().get(0).position);
      // Second read should be the first byte of the second chunk.
      assertEquals(1, ecb.read());

      // Seek to the end of the file minus one byte
      ecb.seek(ONEMB * 5 - 1);
      assertEquals(1, ecb.read());
      assertEquals(ONEMB * 2,
          streamFactory.getBlockStreams().get(1).position);
      // Second read should be EOF as there is no data left
      assertEquals(-1, ecb.read());
      assertEquals(0, ecb.getRemaining());
    }
  }

  @Test
  public void testErrorReadingBlockReportsBadLocation() throws IOException {
    repConfig = new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS,
        ONEMB);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, 5 * ONEMB);
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    try (ECBlockInputStream ecb = createStream(keyInfo, streamFactory, clientConfig)) {
      // Read a full stripe to ensure all streams are created in the stream
      // factory
      ByteBuffer buf = ByteBuffer.allocate(3 * ONEMB);
      int read = ecb.read(buf);
      assertEquals(3 * ONEMB, read);
      // Now make replication index 2 error on the next read
      streamFactory.getBlockStreams().get(1).setThrowException(true);
      buf.clear();
      BadDataLocationException e =
          assertThrows(BadDataLocationException.class, () -> ecb.read(buf));
      assertEquals(1, e.getFailedLocations().size());
      assertEquals(2,
          keyInfo.getPipeline().getReplicaIndex(e.getFailedLocations().get(0)));
    }
  }

  @Test
  public void testNoErrorIfSpareLocationToRead() throws IOException {
    repConfig = new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS,
        ONEMB);
    Map<DatanodeDetails, Integer> datanodes = new LinkedHashMap<>();
    for (int i = 1; i <= repConfig.getRequiredNodes(); i++) {
      datanodes.put(MockDatanodeDetails.randomDatanodeDetails(), i);
    }
    // Add a second index = 1
    datanodes.put(MockDatanodeDetails.randomDatanodeDetails(), 1);

    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 8 * ONEMB, datanodes);
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    try (ECBlockInputStream ecb = createStream(keyInfo, streamFactory, clientConfig)) {
      // Read a full stripe to ensure all streams are created in the stream
      // factory
      ByteBuffer buf = ByteBuffer.allocate(3 * ONEMB);
      int read = ecb.read(buf);
      assertEquals(3 * ONEMB, read);
      // Now make replication index 1 error on the next read but as there is a
      // spare it should read from it with no errors
      streamFactory.getBlockStreams().get(0).setThrowException(true);
      buf.clear();
      read = ecb.read(buf);
      assertEquals(3 * ONEMB, read);

      // Now make the spare one error on the next read, and we should get an
      // error with two failed locations. As each stream is created, a new
      // stream will be created in the stream factory. Our read will read from
      // DNs with EC indexes 1 - 3 first, creating streams 0 to 2. Then when
      // stream(0) is failed for index=1 a new steam is created for the
      // alternative index=1 at stream(3). Hence, to make it error we set
      // stream(3) to throw as below.
      streamFactory.getBlockStreams().get(3).setThrowException(true);
      buf.clear();
      BadDataLocationException e =
          assertThrows(BadDataLocationException.class, () -> ecb.read(buf));
      List<DatanodeDetails> failed = e.getFailedLocations();
      // Expect 2 different DNs reported as failure
      assertEquals(2, failed.size());
      assertNotEquals(failed.get(0), failed.get(1));
      // Both failures should map to index = 1.
      for (DatanodeDetails dn : failed) {
        assertEquals(1, datanodes.get(dn));
      }
    }
  }

  @Test
  public void testEcPipelineRefreshFunction() {
    repConfig = new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS,
        ONEMB);
    BlockLocationInfo keyInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 5, 5 * ONEMB);

    BlockID blockID = new BlockID(1, 1);
    Map<DatanodeDetails, Integer> dnMap = new HashMap<>();
    for (int i = 1; i <= 5; i++) {
      dnMap.put(MockDatanodeDetails.randomDatanodeDetails(), i);
    }

    // Create a refreshFunction that returns a hard-coded EC pipeline.
    Function<BlockID, BlockLocationInfo> refreshFunction = blkID -> {
      Pipeline pipeline = Pipeline.newBuilder()
          .setReplicationConfig(repConfig)
          .setNodes(new ArrayList<>(dnMap.keySet()))
          .setReplicaIndexes(dnMap)
          .setState(Pipeline.PipelineState.CLOSED)
          .setId(PipelineID.randomId())
          .build();
      BlockLocationInfo blockLocation = new BlockLocationInfo.Builder()
          .setPipeline(pipeline)
          .build();
      return blockLocation;
    };

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    try (ECBlockInputStream ecb = createStream(keyInfo, streamFactory, clientConfig)) {
      Pipeline pipeline =
          ecb.ecPipelineRefreshFunction(3, refreshFunction)
              .apply(blockID)
              .getPipeline();
      // Check the pipeline is built with the correct Datanode
      // with right replicaIndex.
      assertEquals(HddsProtos.ReplicationType.STAND_ALONE,
          pipeline.getReplicationConfig().getReplicationType());
      assertEquals(1, pipeline.getNodes().size());
      assertEquals(3, dnMap.get(pipeline.getNodes().get(0)));
    }
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.SECONDS)
  public void testZeroByteReadThrowsBadDataLocationException() throws Exception {
    repConfig = new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS, ONEMB);
    Map<DatanodeDetails, Integer> datanodes = new LinkedHashMap<>();
    for (int i = 1; i <= repConfig.getRequiredNodes(); i++) {
      datanodes.put(MockDatanodeDetails.randomDatanodeDetails(), i);
    }

    BlockLocationInfo keyInfo = ECStreamTestUtil.createKeyInfo(repConfig, 8 * ONEMB, datanodes);
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);

    try (ECBlockInputStream ecb = createStream(keyInfo, streamFactory, clientConfig)) {

      // Read a full stripe first to initialize and create streams in the factory
      ByteBuffer buf = ByteBuffer.allocate(3 * ONEMB);
      int read = ecb.read(buf);
      assertEquals(3 * ONEMB, read);

      // Simulate the Bug: Force the underlying stream to return 0 bytes (Short Read).
      // Note: If the test stub `TestBlockInputStream` does not currently have
      // a method to simulate a 0-byte read, you should add a simple boolean flag
      // like `simulateZeroByteRead` to that stub class, making its read() return 0.
      streamFactory.getBlockStreams().get(0).setSimulateZeroByteRead(true);

      buf.clear();

      // Assert that instead of spinning infinitely, the short read (0 bytes)
      // immediately triggers the strict validation and throws BadDataLocationException.
      // This exception is essential for the Proxy to initiate the Failover to Reconstruction.
      BadDataLocationException e = assertThrows(BadDataLocationException.class, () -> ecb.read(buf));
      List<DatanodeDetails> failed = e.getFailedLocations();

      // Expect exactly 1 DN reported as failure due to the inconsistent read
      assertEquals(1, failed.size());
      // The failure should map to index = 1 (stream 0)
      assertEquals(1, datanodes.get(failed.get(0)));
    }
  }

  public static Stream<Arguments> parallelReadConfigs() {
    return Stream.of(
        Arguments.of(new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS, 100)),
        Arguments.of(new ECReplicationConfig(6, 3, ECReplicationConfig.EcCodec.RS, 100)),
        Arguments.of(new ECReplicationConfig(10, 4, ECReplicationConfig.EcCodec.RS, 100)));
  }

  @ParameterizedTest
  @MethodSource("parallelReadConfigs")
  public void testParallelReadReturnsWrittenData(ECReplicationConfig ecConfig) throws IOException {
    repConfig = ecConfig;
    int chunk = ecConfig.getEcChunkSize();
    int stripe = chunk * ecConfig.getData();
    // two full stripes and a partial last stripe ending inside its second cell
    int blockLength = 2 * stripe + chunk + chunk / 2;
    ECStreamTestUtil.TestBlockInputStreamFactory dataStreamFactory = createDataStreamFactory(blockLength);
    BlockLocationInfo keyInfo = ECStreamTestUtil.createKeyInfo(ecConfig, ecConfig.getRequiredNodes(), blockLength);
    dataStreamFactory.setCurrentPipeline(keyInfo.getPipeline());

    int[][] ranges = {
        {10, 50},                          // within one cell
        {50, 100},                         // across two cells
        {stripe - 30, 60},                 // across a stripe boundary
        {0, blockLength},                  // the whole block
        {2 * stripe + 10, chunk + 40},     // into the partial last stripe
        {blockLength - 130, 500},          // past the end of the block
    };
    try (ECBlockInputStream ecb = createParallelStream(keyInfo, dataStreamFactory, true)) {
      for (int[] range : ranges) {
        ecb.seek(range[0]);
        ByteBuffer buf = ByteBuffer.allocate(range[1]);
        int expected = Math.min(range[1], blockLength - range[0]);
        assertEquals(expected, ecb.read(buf), "range " + Arrays.toString(range));
        assertDataMatches(buf, range[0], expected);
        assertEquals(range[0] + expected, ecb.getPos());
      }
      assertEquals(-1, ecb.read(ByteBuffer.allocate(10)));

      // sequential reads without a seek, through a byte array
      ecb.seek(0);
      byte[] all = new byte[blockLength];
      int offset = 0;
      int read;
      while ((read = ecb.read(all, offset, Math.min(stripe + chunk / 2, blockLength - offset))) > 0) {
        offset += read;
      }
      assertEquals(blockLength, offset);
      assertDataMatches(ByteBuffer.wrap(all), 0, blockLength);
    }
    assertThat(parallelReadTasks.get()).isPositive();
  }

  @Test
  public void testMultiCellReadReadsIndexesConcurrently() throws IOException {
    repConfig = new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS, 100);
    int blockLength = 3 * 300;
    ECStreamTestUtil.TestBlockInputStreamFactory dataStreamFactory = createDataStreamFactory(blockLength);
    BlockLocationInfo keyInfo = ECStreamTestUtil.createKeyInfo(repConfig, 5, blockLength);
    dataStreamFactory.setCurrentPipeline(keyInfo.getPipeline());
    // each read of the three data indexes blocks until all three are in flight, so a serial read times out
    dataStreamFactory.setReadBarrier(new CountDownLatch(3));

    try (ECBlockInputStream ecb = createParallelStream(keyInfo, dataStreamFactory, true)) {
      ByteBuffer buf = ByteBuffer.allocate(300);
      assertEquals(300, ecb.read(buf));
      assertDataMatches(buf, 0, 300);
    }
    // the calling thread reads the first index itself
    assertEquals(2, parallelReadTasks.get());
  }

  @Test
  public void testSingleCellReadAndDisabledFlagDoNotUseExecutor() throws IOException {
    repConfig = new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS, 100);
    int blockLength = 3 * 300;
    BlockLocationInfo keyInfo = ECStreamTestUtil.createKeyInfo(repConfig, 5, blockLength);

    ECStreamTestUtil.TestBlockInputStreamFactory dataStreamFactory = createDataStreamFactory(blockLength);
    dataStreamFactory.setCurrentPipeline(keyInfo.getPipeline());
    try (ECBlockInputStream ecb = createParallelStream(keyInfo, dataStreamFactory, true)) {
      ecb.seek(120);
      ByteBuffer buf = ByteBuffer.allocate(80);
      assertEquals(80, ecb.read(buf));
      assertDataMatches(buf, 120, 80);
    }
    assertEquals(0, parallelReadTasks.get());

    dataStreamFactory = createDataStreamFactory(blockLength);
    dataStreamFactory.setCurrentPipeline(keyInfo.getPipeline());
    try (ECBlockInputStream ecb = createParallelStream(keyInfo, dataStreamFactory, false)) {
      ByteBuffer buf = ByteBuffer.allocate(blockLength);
      assertEquals(blockLength, ecb.read(buf));
      assertDataMatches(buf, 0, blockLength);
    }
    assertEquals(0, parallelReadTasks.get());
  }

  @Test
  public void testParallelReadReportsAllFailedIndexes() throws IOException {
    repConfig = new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS, 100);
    int blockLength = 3 * 300;
    BlockLocationInfo keyInfo = ECStreamTestUtil.createKeyInfo(repConfig, 5, blockLength);
    ECStreamTestUtil.TestBlockInputStreamFactory dataStreamFactory = createDataStreamFactory(blockLength);
    dataStreamFactory.setCurrentPipeline(keyInfo.getPipeline());
    dataStreamFactory.setFailIndexes(2, 3);

    try (ECBlockInputStream ecb = createParallelStream(keyInfo, dataStreamFactory, true)) {
      ByteBuffer buf = ByteBuffer.allocate(300);
      BadDataLocationException e = assertThrows(BadDataLocationException.class, () -> ecb.read(buf));
      assertEquals(Arrays.asList(1, 2), e.getFailedLocationIndexes());
      assertEquals(2, e.getFailedLocations().size());
      for (DatanodeDetails dn : e.getFailedLocations()) {
        assertThat(keyInfo.getPipeline().getReplicaIndex(dn)).isIn(2, 3);
      }
      // the first cell was read, and the failure of the second must not be hidden by the third
      assertEquals(100, ecb.getPos());
      assertEquals(100, buf.position());
    }
  }

  @Test
  public void testReadAfterFailedParallelReadReturnsCorrectData() throws IOException {
    repConfig = new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS, 100);
    int blockLength = 3 * 300;
    BlockLocationInfo keyInfo = ECStreamTestUtil.createKeyInfo(repConfig, 5, blockLength);
    ECStreamTestUtil.TestBlockInputStreamFactory dataStreamFactory = createDataStreamFactory(blockLength);
    dataStreamFactory.setCurrentPipeline(keyInfo.getPipeline());
    dataStreamFactory.setFailIndexes(2);

    try (ECBlockInputStream ecb = createParallelStream(keyInfo, dataStreamFactory, true)) {
      assertThrows(BadDataLocationException.class, () -> ecb.read(ByteBuffer.allocate(300)));
      assertEquals(100, ecb.getPos());
      // index 3 has read its cell although the position stays before it, so the retry must seek its stream back
      dataStreamFactory.getBlockStream(2).setShouldError(false);
      ByteBuffer buf = ByteBuffer.allocate(200);
      assertEquals(200, ecb.read(buf));
      assertDataMatches(buf, 100, 200);
      assertEquals(300, ecb.getPos());
    }
  }

  @Test
  public void testParallelReadRetriesFailedIndexOnSpare() throws IOException {
    repConfig = new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS, 100);
    int blockLength = 3 * 300;
    Map<DatanodeDetails, Integer> datanodes = new LinkedHashMap<>();
    for (int i = 1; i <= repConfig.getRequiredNodes(); i++) {
      datanodes.put(MockDatanodeDetails.randomDatanodeDetails(), i);
    }
    // a spare location for index 2
    datanodes.put(MockDatanodeDetails.randomDatanodeDetails(), 2);
    BlockLocationInfo keyInfo = ECStreamTestUtil.createKeyInfo(repConfig, blockLength, datanodes);
    ECStreamTestUtil.TestBlockInputStreamFactory dataStreamFactory = createDataStreamFactory(blockLength);
    dataStreamFactory.setCurrentPipeline(keyInfo.getPipeline());
    dataStreamFactory.setFailIndexes(2);

    try (ECBlockInputStream ecb = createParallelStream(keyInfo, dataStreamFactory, true)) {
      ecb.seek(50);
      ByteBuffer buf = ByteBuffer.allocate(600);
      assertEquals(600, ecb.read(buf));
      assertDataMatches(buf, 50, 600);
      assertEquals(650, ecb.getPos());
    }
  }

  /** Creates the stream under test, reading the cells in parallel if the class runs with parallelRead. */
  private ECBlockInputStream createStream(BlockLocationInfo keyInfo, BlockInputStreamFactory factory,
      OzoneClientConfig clientConfig) {
    clientConfig.setEcParallelReadEnabled(parallelRead);
    return new ECBlockInputStream(repConfig, keyInfo, null, null, factory, clientConfig, () -> parallelReadExecutor);
  }

  private ECBlockInputStream createParallelStream(BlockLocationInfo keyInfo,
      BlockInputStreamFactory factory, boolean parallelReadEnabled) {
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setEcParallelReadEnabled(parallelReadEnabled);
    return new ECBlockInputStream(repConfig, keyInfo, null, null, factory, clientConfig,
        () -> parallelReadExecutor);
  }

  /**
   * Creates a stream factory whose data index streams hold the block data, where the byte at block offset p is
   * {@link #dataAt(long)}.
   */
  private ECStreamTestUtil.TestBlockInputStreamFactory createDataStreamFactory(int blockLength) {
    List<ByteBuffer> indexData = new ArrayList<>();
    for (int i = 1; i <= repConfig.getRequiredNodes(); i++) {
      indexData.add(ByteBuffer.allocate((int) ECBlockInputStream.internalBlockLength(i, repConfig, blockLength)));
    }
    int chunk = repConfig.getEcChunkSize();
    for (long p = 0; p < blockLength; p++) {
      int index = (int) ((p / chunk) % repConfig.getData());
      indexData.get(index).put(dataAt(p));
    }
    ECStreamTestUtil.TestBlockInputStreamFactory factory = new ECStreamTestUtil.TestBlockInputStreamFactory();
    factory.setBlockStreamData(indexData);
    return factory;
  }

  private static byte dataAt(long blockOffset) {
    return (byte) (blockOffset * 31 + 7);
  }

  private static void assertDataMatches(ByteBuffer buf, long blockOffset, int length) {
    for (int i = 0; i < length; i++) {
      assertEquals(dataAt(blockOffset + i), buf.get(i), "byte " + i + " at block offset " + (blockOffset + i));
    }
  }

  @Test
  void testPositionedReadsReuseOneStreamPerLocation() throws IOException {
    ECReplicationConfig smallRepConfig = new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS, 1024);
    int chunk = smallRepConfig.getEcChunkSize();
    int blockLength = chunk * 7 - 101;
    ByteBuffer[] data = new ByteBuffer[smallRepConfig.getData()];
    for (int i = 0; i < data.length; i++) {
      data[i] = ByteBuffer.allocate(chunk * 3);
    }
    ECStreamTestUtil.randomFill(data, chunk, new SplittableRandom(7), blockLength);
    byte[] expected = new byte[blockLength];
    ECStreamTestUtil.randomFill(ByteBuffer.wrap(expected), new SplittableRandom(7));
    List<ByteBuffer> locationData = new ArrayList<>();
    for (ByteBuffer b : data) {
      b.flip();
      locationData.add(b);
    }
    BlockLocationInfo keyInfo = ECStreamTestUtil.createKeyInfo(smallRepConfig, blockLength,
        ECStreamTestUtil.createIndexMap(1, 2, 3, 4, 5));
    ECStreamTestUtil.TestBlockInputStreamFactory factory = new ECStreamTestUtil.TestBlockInputStreamFactory();
    factory.setCurrentPipeline(keyInfo.getPipeline());
    factory.setBlockStreamData(locationData);

    repConfig = smallRepConfig;
    try (ECBlockInputStream ecb = createStream(keyInfo, factory, conf.getObject(OzoneClientConfig.class))) {
      ecb.seek(11);
      // Offsets within a cell, across cell and stripe boundaries, and into the partial last stripe.
      int[][] reads = {{0, 10}, {chunk - 5, 10}, {chunk * 2 - 3, chunk + 6}, {chunk * 3 - 1, 2},
          {chunk * 4 + 17, chunk * 2}, {blockLength - 9, 9}, {5, blockLength - 5}};
      for (int[] read : reads) {
        ByteBuffer buffer = ByteBuffer.allocate(read[1]);
        ecb.readFully(read[0], buffer);
        assertArrayEquals(Arrays.copyOfRange(expected, read[0], read[0] + read[1]), buffer.array());
      }
      // A buffer longer than the rest of the block is filled up to the end of the block.
      ByteBuffer tail = ByteBuffer.allocate(20);
      assertEquals(9, ecb.read(blockLength - 9, tail));
      assertEquals(9, tail.position());
      assertArrayEquals(Arrays.copyOfRange(expected, blockLength - 9, blockLength), Arrays.copyOf(tail.array(), 9));
      // The length is clamped by the block, not by what remains after the sequential cursor.
      ecb.seek(blockLength - 1);
      ByteBuffer mid = ByteBuffer.allocate(chunk + 6);
      assertEquals(chunk + 6, ecb.read(chunk * 2 - 3, mid));
      assertArrayEquals(Arrays.copyOfRange(expected, chunk * 2 - 3, chunk * 3 + 3), mid.array());
      ecb.seek(11);
      assertEquals(-1, ecb.read(blockLength, ByteBuffer.allocate(1)));
      assertEquals(11, ecb.getPos());
      // one stream per location, none of them replaced by a later read
      assertEquals(smallRepConfig.getData(), factory.getCreatedCount());
      assertEquals(parallelRead, parallelReadTasks.get() > 0);

      // A failing location surfaces as BadDataLocationException naming that location; its stream is not reopened.
      ECStreamTestUtil.TestBlockInputStream failedStream = factory.getBlockStream(2);
      failedStream.setShouldError(true);
      BadDataLocationException e = assertThrows(BadDataLocationException.class,
          () -> ecb.read(chunk + 1, ByteBuffer.allocate(10)));
      assertEquals(Collections.singletonList(1), e.getFailedLocationIndexes());
      assertEquals(failedStream, factory.getBlockStream(2));
      // Simultaneous failures on several indexes are all reported.
      factory.getBlockStream(3).setShouldError(true);
      e = assertThrows(BadDataLocationException.class, () -> ecb.read(chunk + 1, ByteBuffer.allocate(chunk * 2)));
      assertEquals(Arrays.asList(1, 2), e.getFailedLocationIndexes());
    }
  }

  @Test
  void testPositionedReadSwitchesToASpare() throws IOException {
    DatanodeDetails spare = MockDatanodeDetails.randomDatanodeDetails();
    int blockLength = 3 * 300;
    BlockLocationInfo keyInfo = createKeyInfoWithSpare(blockLength, spare);
    ECStreamTestUtil.TestBlockInputStreamFactory factory = createDataStreamFactory(blockLength);
    factory.setCurrentPipeline(keyInfo.getPipeline());

    try (ECBlockInputStream ecb = createStream(keyInfo, factory, conf.getObject(OzoneClientConfig.class))) {
      ByteBuffer buf = ByteBuffer.allocate(50);
      assertEquals(50, ecb.read(120, buf));
      assertDataMatches(buf, 120, 50);
      DatanodeDetails original = ecb.getDataLocations()[1];
      assertNotEquals(spare, original);
      ECStreamTestUtil.TestBlockInputStream positionedStream = factory.getBlockStream(2);

      // A failed positioned read closes the failed stream, switches the index to its spare and retries from it,
      // without touching the sequential locations.
      positionedStream.setShouldError(true);
      buf = ByteBuffer.allocate(50);
      assertEquals(50, ecb.read(120, buf));
      assertDataMatches(buf, 120, 50);
      assertTrue(positionedStream.isClosed());
      ECStreamTestUtil.TestBlockInputStream spareStream = factory.getBlockStream(2);
      assertNotEquals(positionedStream, spareStream);
      assertEquals(2, factory.getCreatedCount());
      assertEquals(original, ecb.getDataLocations()[1]);

      // With no spare left, the positioned read fails naming the spare and the original location, so that the
      // reconstruction read ECBlockInputStreamProxy fails over to uses neither; the spare stream is not reopened.
      spareStream.setShouldError(true);
      BadDataLocationException e = assertThrows(BadDataLocationException.class,
          () -> ecb.read(120, ByteBuffer.allocate(50)));
      assertEquals(Collections.singletonList(1), e.getFailedLocationIndexes());
      assertEquals(Arrays.asList(spare, original), e.getFailedLocations());
      assertEquals(spareStream, factory.getBlockStream(2));
      assertEquals(2, factory.getCreatedCount());

      // The sequential read uses its own streams and its own spare, which the positioned reads have not consumed.
      factory.setFailIndexes(2);
      buf = ByteBuffer.allocate(300);
      assertEquals(300, ecb.read(buf));
      assertDataMatches(buf, 0, 300);
      assertEquals(spare, ecb.getDataLocations()[1]);
    }
  }

  @Test
  @Timeout(30)
  void testPositionedReadFailedUnderASpareSwitchRetriesFromTheSpare() throws Exception {
    DatanodeDetails spare = MockDatanodeDetails.randomDatanodeDetails();
    int blockLength = 3 * 300;
    BlockLocationInfo keyInfo = createKeyInfoWithSpare(blockLength, spare);
    ECStreamTestUtil.TestBlockInputStreamFactory factory = createDataStreamFactory(blockLength);
    factory.setCurrentPipeline(keyInfo.getPipeline());

    ExecutorService pool = Executors.newSingleThreadExecutor();
    try (ECBlockInputStream ecb = createStream(keyInfo, factory, conf.getObject(OzoneClientConfig.class))) {
      DatanodeDetails original = ecb.getDataLocations()[1];
      // a positioned read is parked inside the stream of index 2
      CountDownLatch barrier = new CountDownLatch(2);
      factory.setReadBarrier(barrier);
      ByteBuffer parked = ByteBuffer.allocate(50);
      Future<Integer> parkedRead = pool.submit(() -> ecb.read(120, parked));
      while (barrier.getCount() > 1) {
        Thread.sleep(1);
      }
      factory.setReadBarrier(null);
      ECStreamTestUtil.TestBlockInputStream originalStream = factory.getBlockStream(2);

      // Another positioned read fails on the same stream, switches index 2 to its spare, closing the stream under
      // the parked read, and retries from the spare.
      originalStream.setShouldError(true);
      ByteBuffer buf = ByteBuffer.allocate(50);
      assertEquals(50, ecb.read(120, buf));
      assertDataMatches(buf, 120, 50);
      assertTrue(originalStream.isClosed());
      ECStreamTestUtil.TestBlockInputStream spareStream = factory.getBlockStream(2);
      assertNotEquals(originalStream, spareStream);
      assertEquals(2, factory.getCreatedCount());

      // The parked read fails because its stream was closed. The failure is charged to the location the stream was
      // opened against, which has been switched away from already, so the read is retried as it is from the spare,
      // without blaming the spare, consuming another spare or failing the index.
      originalStream.setShouldError(false);
      barrier.countDown();
      assertEquals(50, parkedRead.get(10, TimeUnit.SECONDS));
      assertDataMatches(parked, 120, 50);
      assertEquals(spareStream, factory.getBlockStream(2));
      assertFalse(spareStream.isClosed());
      assertEquals(2, factory.getCreatedCount());

      // Only the original location was recorded as failed; the spare is named once it fails itself.
      spareStream.setShouldError(true);
      BadDataLocationException e = assertThrows(BadDataLocationException.class,
          () -> ecb.read(120, ByteBuffer.allocate(50)));
      assertEquals(Arrays.asList(spare, original), e.getFailedLocations());
      assertEquals(2, factory.getCreatedCount());
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void testPositionedReadStreamsFollowTheStreamLifecycle() throws IOException {
    repConfig = new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS, 100);
    int blockLength = 3 * 300;
    BlockLocationInfo keyInfo = ECStreamTestUtil.createKeyInfo(repConfig, 5, blockLength);
    ECStreamTestUtil.TestBlockInputStreamFactory factory = createDataStreamFactory(blockLength);
    factory.setCurrentPipeline(keyInfo.getPipeline());

    ECBlockInputStream ecb = createStream(keyInfo, factory, conf.getObject(OzoneClientConfig.class));
    // a read spanning every data index opens every pread stream
    ByteBuffer buf = ByteBuffer.allocate(300);
    assertEquals(300, ecb.read(0, buf));
    assertDataMatches(buf, 0, 300);
    assertEquals(repConfig.getData(), factory.getCreatedCount());
    // a sequential read spanning every data index opens the sequential streams, separate from the pread streams
    List<ECStreamTestUtil.TestBlockInputStream> streams = factory.getBlockStreams();
    buf = ByteBuffer.allocate(300);
    assertEquals(300, ecb.read(buf));
    assertDataMatches(buf, 0, 300);
    assertEquals(2 * repConfig.getData(), factory.getCreatedCount());
    streams.addAll(factory.getBlockStreams());
    assertEquals(2 * repConfig.getData(), streams.size());

    ecb.unbuffer();
    for (ECStreamTestUtil.TestBlockInputStream stream : streams) {
      assertEquals(1, stream.getUnbufferCalls());
      assertFalse(stream.isClosed());
    }

    ecb.close();
    for (ECStreamTestUtil.TestBlockInputStream stream : streams) {
      assertTrue(stream.isClosed());
    }
    // A positioned read after close fails without reopening a stream.
    IOException e = assertThrows(IOException.class, () -> ecb.read(0, ByteBuffer.allocate(1)));
    assertThat(e.getMessage()).contains(FSExceptionMessages.STREAM_IS_CLOSED);
    assertEquals(2 * repConfig.getData(), factory.getCreatedCount());
  }

  @Test
  @Timeout(30)
  void testUnbufferHoldsNoMonitorWhileWaitingOnAPositionedRead() throws Exception {
    repConfig = new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS, 100);
    int blockLength = 3 * 300;
    BlockLocationInfo keyInfo = ECStreamTestUtil.createKeyInfo(repConfig, 5, blockLength);
    ECStreamTestUtil.TestBlockInputStreamFactory factory = createDataStreamFactory(blockLength);
    factory.setCurrentPipeline(keyInfo.getPipeline());

    ExecutorService pool = Executors.newFixedThreadPool(2);
    try (ECBlockInputStream ecb = createStream(keyInfo, factory, conf.getObject(OzoneClientConfig.class))) {
      ByteBuffer buf = ByteBuffer.allocate(300);
      assertEquals(300, ecb.read(buf));
      List<ECStreamTestUtil.TestBlockInputStream> sequentialStreams = factory.getBlockStreams();
      // a positioned read waits inside its stream, holding the monitor of the stream, as in a slow BlockInputStream
      CountDownLatch barrier = new CountDownLatch(2);
      factory.setReadBarrier(barrier);
      ByteBuffer parked = ByteBuffer.allocate(50);
      Future<Integer> parkedRead = pool.submit(() -> ecb.read(120, parked));
      while (barrier.getCount() > 1) {
        Thread.sleep(1);
      }
      factory.setReadBarrier(null);
      ECStreamTestUtil.TestBlockInputStream parkedStream = factory.getBlockStream(2);

      // unbuffer() unbuffers the sequential streams, then waits for the parked stream without holding any monitor,
      // so a sequential read and a positioned read of another index complete meanwhile.
      Future<?> unbuffer = pool.submit(ecb::unbuffer);
      while (sequentialStreams.stream().anyMatch(stream -> stream.getUnbufferCalls() == 0)) {
        Thread.sleep(1);
      }
      ecb.seek(0);
      buf = ByteBuffer.allocate(300);
      assertEquals(300, ecb.read(buf));
      assertDataMatches(buf, 0, 300);
      buf = ByteBuffer.allocate(50);
      assertEquals(50, ecb.read(220, buf));
      assertDataMatches(buf, 220, 50);
      assertFalse(unbuffer.isDone());

      barrier.countDown();
      unbuffer.get(10, TimeUnit.SECONDS);
      assertEquals(50, parkedRead.get(10, TimeUnit.SECONDS));
      assertDataMatches(parked, 120, 50);
      assertEquals(1, parkedStream.getUnbufferCalls());
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  @Timeout(30)
  void testCloseDuringPositionedReadFailsAsClosed() throws Exception {
    repConfig = new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS, 100);
    int blockLength = 3 * 300;
    BlockLocationInfo keyInfo = ECStreamTestUtil.createKeyInfo(repConfig, 5, blockLength);
    ECStreamTestUtil.TestBlockInputStreamFactory factory = createDataStreamFactory(blockLength);
    factory.setCurrentPipeline(keyInfo.getPipeline());
    // the positioned read waits in its stream until the stream has been closed under it
    CountDownLatch barrier = new CountDownLatch(2);
    factory.setReadBarrier(barrier);

    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      ECBlockInputStream ecb = createStream(keyInfo, factory, conf.getObject(OzoneClientConfig.class));
      Future<IOException> failure = pool.submit(
          () -> assertThrows(IOException.class, () -> ecb.read(120, ByteBuffer.allocate(50))));
      while (barrier.getCount() > 1) {
        Thread.sleep(1);
      }
      ecb.close();
      assertTrue(factory.getBlockStream(2).isClosed());
      barrier.countDown();
      // The read fails because the stream is closed, not because of a bad location.
      IOException e = failure.get();
      assertFalse(e instanceof BadDataLocationException, e.toString());
      assertThat(e.getMessage()).contains(FSExceptionMessages.STREAM_IS_CLOSED);
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void testConcurrentPositionedReads() throws Exception {
    repConfig = new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS, 1024);
    int stripe = repConfig.getEcChunkSize() * repConfig.getData();
    int blockLength = 5 * stripe + 500;
    byte[] expected = new byte[blockLength];
    for (int i = 0; i < blockLength; i++) {
      expected[i] = dataAt(i);
    }
    BlockLocationInfo keyInfo = ECStreamTestUtil.createKeyInfo(repConfig, 5, blockLength);
    ECStreamTestUtil.TestBlockInputStreamFactory factory = createDataStreamFactory(blockLength);
    factory.setCurrentPipeline(keyInfo.getPipeline());

    try (ECBlockInputStream ecb = createStream(keyInfo, factory, conf.getObject(OzoneClientConfig.class))) {
      ecb.seek(11);
      PositionedReadTestHelper.runConcurrentPositionedReads(expected, (offset, buf) -> ecb.readFully(offset, buf));
      assertEquals(11, ecb.getPos());
      // every thread shares the one pread stream of each location
      assertEquals(repConfig.getData(), factory.getCreatedCount());
    }
  }

  @Test
  void testPositionedReadReportsFailedStreamCreation() throws IOException {
    repConfig = new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS, 100);
    int blockLength = 3 * 300;
    BlockLocationInfo keyInfo = ECStreamTestUtil.createKeyInfo(repConfig, 5, blockLength);
    ECStreamTestUtil.TestBlockInputStreamFactory factory = createDataStreamFactory(blockLength);
    factory.setCurrentPipeline(keyInfo.getPipeline());
    factory.setFailCreateIndexes(2);

    try (ECBlockInputStream ecb = createStream(keyInfo, factory, conf.getObject(OzoneClientConfig.class))) {
      BadDataLocationException e = assertThrows(BadDataLocationException.class,
          () -> ecb.read(120, ByteBuffer.allocate(50)));
      assertEquals(Collections.singletonList(1), e.getFailedLocationIndexes());
      assertEquals(Collections.singletonList(ecb.getDataLocations()[1]), e.getFailedLocations());
      assertEquals("create failed", e.getCause().getMessage());
      // A failed creation does not poison the slot: the next read creates the stream.
      ByteBuffer buf = ByteBuffer.allocate(50);
      assertEquals(50, ecb.read(120, buf));
      assertDataMatches(buf, 120, 50);
      assertEquals(1, factory.getCreatedCount());
    }
  }

  /** Creates the key info for a block with an extra, spare location for index 2. */
  private BlockLocationInfo createKeyInfoWithSpare(int blockLength, DatanodeDetails spare) {
    repConfig = new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS, 100);
    Map<DatanodeDetails, Integer> datanodes = new LinkedHashMap<>();
    for (int i = 1; i <= repConfig.getRequiredNodes(); i++) {
      datanodes.put(MockDatanodeDetails.randomDatanodeDetails(), i);
    }
    datanodes.put(spare, 2);
    return ECStreamTestUtil.createKeyInfo(repConfig, blockLength, datanodes);
  }

  private void validateBufferContents(ByteBuffer buf, int from, int to,
      byte val) {
    for (int i = from; i < to; i++) {
      assertEquals(val, buf.get(i));
    }
  }

  private static class TestBlockInputStreamFactory implements
      BlockInputStreamFactory {

    private List<TestBlockInputStream> blockStreams = new ArrayList<>();

    public synchronized List<TestBlockInputStream> getBlockStreams() {
      return blockStreams;
    }

    @Override
    public synchronized BlockExtendedInputStream create(
        ReplicationConfig repConfig, BlockLocationInfo blockInfo,
        Pipeline pipeline, Token<OzoneBlockTokenIdentifier> token,
        XceiverClientFactory xceiverFactory,
        Function<BlockID, BlockLocationInfo> refreshFunction,
        OzoneClientConfig config) {
      TestBlockInputStream stream = new TestBlockInputStream(
          blockInfo.getBlockID(), blockInfo.getLength(),
          (byte)blockStreams.size());
      blockStreams.add(stream);
      return stream;
    }
  }

  private static class TestBlockInputStream extends BlockExtendedInputStream {

    private long position = 0;
    private boolean closed = false;
    private byte dataVal = 1;
    private BlockID blockID;
    private long length;
    private boolean throwException = false;
    private boolean simulateZeroByteRead = false;
    private static final byte EOF = -1;

    @SuppressWarnings("checkstyle:parameternumber")
    TestBlockInputStream(BlockID blockId, long blockLen, byte dataVal) {
      this.dataVal = dataVal;
      this.blockID = blockId;
      this.length = blockLen;
    }

    public boolean isClosed() {
      return closed;
    }

    public void setThrowException(boolean shouldThrow) {
      this.throwException = shouldThrow;
    }

    public void setSimulateZeroByteRead(boolean simulateZeroByteRead) {
      this.simulateZeroByteRead = simulateZeroByteRead;
    }

    @Override
    protected int readPositioned(long offset, ByteBuffer buffer) {
      if (offset >= length) {
        return EOF;
      }
      int n = (int) Math.min(buffer.remaining(), length - offset);
      for (int i = 0; i < n; i++) {
        buffer.put(dataVal);
      }
      return n;
    }

    @Override
    public BlockID getBlockID() {
      return blockID;
    }

    @Override
    public long getLength() {
      return length;
    }

    @Override
    public int read(byte[] b, int off, int len)
        throws IOException {
      return read(ByteBuffer.wrap(b, off, len));
    }

    @Override
    public int read(ByteBuffer buf) throws IOException {
      if (getRemaining() == 0) {
        return EOF;
      }

      if (throwException) {
        throw new IOException("Simulated exception");
      }

      if (simulateZeroByteRead) {
        return 0;
      }

      int toRead = Math.min(buf.remaining(), (int)getRemaining());
      for (int i = 0; i < toRead; i++) {
        buf.put(dataVal);
      }
      position += toRead;
      return toRead;
    }

    @Override
    protected int readWithStrategy(ByteReaderStrategy strategy) throws
        IOException {
      throw new IOException("Should not be called");
    }

    @Override
    public void close() {
      closed = true;
    }

    @Override
    public void unbuffer() {
    }

    @Override
    public void seek(long pos) {
      this.position = pos;
    }

    @Override
    public long getPos() {
      return position;
    }
  }

}
