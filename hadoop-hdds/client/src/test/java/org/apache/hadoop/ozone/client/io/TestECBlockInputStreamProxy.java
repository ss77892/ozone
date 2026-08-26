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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.storage.BlockExtendedInputStream;
import org.apache.hadoop.hdds.scm.storage.BlockLocationInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the  ECBlockInputStreamProxy class.
 */
public class TestECBlockInputStreamProxy {

  private static final int ONEMB = 1024 * 1024;
  private ECReplicationConfig repConfig;
  private TestECBlockInputStreamFactory streamFactory;

  private long randomSeed;
  private ThreadLocalRandom random = ThreadLocalRandom.current();
  private SplittableRandom dataGenerator;
  private OzoneConfiguration conf = new OzoneConfiguration();

  @BeforeEach
  public void setup() {
    repConfig = new ECReplicationConfig(3, 2);
    streamFactory = new TestECBlockInputStreamFactory();
    randomSeed = random.nextLong();
    dataGenerator = new SplittableRandom(randomSeed);
  }

  @Test
  public void testExpectedDataLocations() {
    assertEquals(1,
        ECBlockInputStreamProxy.expectedDataLocations(repConfig, 1));
    assertEquals(2,
        ECBlockInputStreamProxy.expectedDataLocations(repConfig, ONEMB + 1));
    assertEquals(3,
        ECBlockInputStreamProxy.expectedDataLocations(repConfig, 3 * ONEMB));
    assertEquals(3,
        ECBlockInputStreamProxy.expectedDataLocations(repConfig, 10 * ONEMB));

    repConfig = new ECReplicationConfig(6, 3);
    assertEquals(1,
        ECBlockInputStreamProxy.expectedDataLocations(repConfig, 1));
    assertEquals(2,
        ECBlockInputStreamProxy.expectedDataLocations(repConfig, ONEMB + 1));
    assertEquals(3,
        ECBlockInputStreamProxy.expectedDataLocations(repConfig, 3 * ONEMB));
    assertEquals(6,
        ECBlockInputStreamProxy.expectedDataLocations(repConfig, 10 * ONEMB));
  }

  @Test
  public void testAvailableDataLocations() {
    Map<DatanodeDetails, Integer> dnMap =
        ECStreamTestUtil.createIndexMap(1, 2, 3, 4, 5);
    BlockLocationInfo blockInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, 1024, dnMap);
    assertEquals(1, ECBlockInputStreamProxy.availableDataLocations(
        blockInfo.getPipeline(), 1));
    assertEquals(2, ECBlockInputStreamProxy.availableDataLocations(
        blockInfo.getPipeline(), 2));
    assertEquals(3, ECBlockInputStreamProxy.availableDataLocations(
        blockInfo.getPipeline(), 3));

    dnMap = ECStreamTestUtil.createIndexMap(1, 4, 5);
    blockInfo = ECStreamTestUtil.createKeyInfo(repConfig, 1024, dnMap);
    assertEquals(1, ECBlockInputStreamProxy.availableDataLocations(
        blockInfo.getPipeline(), 3));

    dnMap = ECStreamTestUtil.createIndexMap(2, 3, 4, 5);
    blockInfo = ECStreamTestUtil.createKeyInfo(repConfig, 1024, dnMap);
    assertEquals(0, ECBlockInputStreamProxy.availableDataLocations(
        blockInfo.getPipeline(), 1));
  }

  @Test
  public void testBlockIDCanBeRetrieved() throws IOException {
    int blockLength = 1234;
    generateData(blockLength);

    Map<DatanodeDetails, Integer> dnMap =
        ECStreamTestUtil.createIndexMap(1, 2, 3, 4, 5);
    BlockLocationInfo blockInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, blockLength, dnMap);

    try (ECBlockInputStreamProxy bis = createBISProxy(repConfig, blockInfo)) {
      assertEquals(blockInfo.getBlockID(), bis.getBlockID());
    }
  }

  @Test
  public void testBlockLengthCanBeRetrieved() throws IOException {
    int blockLength = 1234;
    generateData(blockLength);

    Map<DatanodeDetails, Integer> dnMap =
        ECStreamTestUtil.createIndexMap(1, 2, 3, 4, 5);
    BlockLocationInfo blockInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, blockLength, dnMap);

    try (ECBlockInputStreamProxy bis = createBISProxy(repConfig, blockInfo)) {
      assertEquals(1234, bis.getLength());
    }
  }

  @Test
  public void testBlockRemainingCanBeRetrieved() throws IOException {
    int blockLength = 12345;
    generateData(blockLength);

    Map<DatanodeDetails, Integer> dnMap =
        ECStreamTestUtil.createIndexMap(1, 2, 3, 4, 5);
    BlockLocationInfo blockInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, blockLength, dnMap);

    dataGenerator = new SplittableRandom(randomSeed);
    ByteBuffer readBuffer = ByteBuffer.allocate(100);
    try (ECBlockInputStreamProxy bis = createBISProxy(repConfig, blockInfo)) {
      assertEquals(12345, bis.getRemaining());
      assertEquals(0, bis.getPos());
      bis.read(readBuffer);
      assertEquals(12345 - 100, bis.getRemaining());
      assertEquals(100, bis.getPos());
    }
  }

  @Test
  public void testCorrectStreamCreatedDependingOnDataLocations()
      throws IOException {
    int blockLength = 5 * ONEMB;
    ByteBuffer data = generateData(blockLength);

    Map<DatanodeDetails, Integer> dnMap =
        ECStreamTestUtil.createIndexMap(1, 2, 3, 4, 5);
    BlockLocationInfo blockInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, blockLength, dnMap);

    try (ECBlockInputStreamProxy ignored = createBISProxy(repConfig, blockInfo)) {
      // Not all locations present, so we expect on;y the "missing=true" stream
      // to be present.
      assertThat(streamFactory.getStreams()).containsKey(false);
      assertThat(streamFactory.getStreams()).doesNotContainKey(true);
    }

    streamFactory = new TestECBlockInputStreamFactory();
    streamFactory.setData(data);
    dnMap = ECStreamTestUtil.createIndexMap(2, 3, 4, 5);
    blockInfo = ECStreamTestUtil.createKeyInfo(repConfig, blockLength, dnMap);

    try (ECBlockInputStreamProxy ignored = createBISProxy(repConfig, blockInfo)) {
      // Not all locations present, so we expect on;y the "missing=true" stream
      // to be present.
      assertThat(streamFactory.getStreams()).doesNotContainKey(false);
      assertThat(streamFactory.getStreams()).containsKey(true);
    }
  }

  @Test
  public void testCanReadNonReconstructionToEOF()
      throws IOException {
    int blockLength = 5 * ONEMB;
    generateData(blockLength);

    Map<DatanodeDetails, Integer> dnMap =
        ECStreamTestUtil.createIndexMap(1, 2, 3, 4, 5);
    BlockLocationInfo blockInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, blockLength, dnMap);

    ByteBuffer readBuffer = ByteBuffer.allocate(100);
    dataGenerator = new SplittableRandom(randomSeed);
    try (ECBlockInputStreamProxy bis = createBISProxy(repConfig, blockInfo)) {
      while (true) {
        int read = bis.read(readBuffer);
        ECStreamTestUtil.assertBufferMatches(readBuffer, dataGenerator);
        readBuffer.clear();
        if (read < 100) {
          break;
        }
      }
      readBuffer.clear();
      int read = bis.read(readBuffer);
      assertEquals(-1, read);
    }
  }

  @Test
  public void testCanReadReconstructionToEOF()
      throws IOException {
    int blockLength = 5 * ONEMB;
    generateData(blockLength);

    Map<DatanodeDetails, Integer> dnMap =
        ECStreamTestUtil.createIndexMap(2, 3, 4, 5);
    BlockLocationInfo blockInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, blockLength, dnMap);

    ByteBuffer readBuffer = ByteBuffer.allocate(100);
    dataGenerator = new SplittableRandom(randomSeed);
    try (ECBlockInputStreamProxy bis = createBISProxy(repConfig, blockInfo)) {
      while (true) {
        int read = bis.read(readBuffer);
        ECStreamTestUtil.assertBufferMatches(readBuffer, dataGenerator);
        readBuffer.clear();
        if (read < 100) {
          break;
        }
      }
      readBuffer.clear();
      int read = bis.read(readBuffer);
      assertEquals(-1, read);
    }
  }

  @Test
  public void testCanHandleErrorAndFailOverToReconstruction()
      throws IOException {
    int blockLength = 5 * ONEMB;
    generateData(blockLength);

    Map<DatanodeDetails, Integer> dnMap =
        ECStreamTestUtil.createIndexMap(1, 2, 3, 4, 5);
    BlockLocationInfo blockInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, blockLength, dnMap);

    ByteBuffer readBuffer = ByteBuffer.allocate(100);
    DatanodeDetails badDN = blockInfo.getPipeline().getFirstNode();

    dataGenerator = new SplittableRandom(randomSeed);
    try (ECBlockInputStreamProxy bis = createBISProxy(repConfig, blockInfo)) {
      // Perform one read to get the stream created
      int read = bis.read(readBuffer);
      assertEquals(100, read);
      ECStreamTestUtil.assertBufferMatches(readBuffer, dataGenerator);
      // Setup an error to be thrown part through a read, so the dataBuffer
      // will have been advanced by 50 bytes before the error. This tests it
      // correctly rewinds and the same data is loaded again from the other
      // stream.
      streamFactory.getStreams().get(false).setShouldError(true, 151,
          new BadDataLocationException(badDN, "Simulated Error"));
      while (true) {
        readBuffer.clear();
        read = bis.read(readBuffer);
        ECStreamTestUtil.assertBufferMatches(readBuffer, dataGenerator);
        if (read < 100) {
          break;
        }
      }
      readBuffer.clear();
      read = bis.read(readBuffer);
      assertEquals(-1, read);
      // Ensure the bad location was passed into the factory to create the
      // reconstruction reader
      assertEquals(badDN, streamFactory.getFailedLocations().get(0));
    }
  }

  @Test
  public void testCanSeekToNewPosition() throws IOException {
    int blockLength = 5 * ONEMB;
    generateData(blockLength);

    Map<DatanodeDetails, Integer> dnMap =
        ECStreamTestUtil.createIndexMap(1, 2, 3, 4, 5);
    BlockLocationInfo blockInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, blockLength, dnMap);

    ByteBuffer readBuffer = ByteBuffer.allocate(100);
    dataGenerator = new SplittableRandom(randomSeed);
    try (ECBlockInputStreamProxy bis = createBISProxy(repConfig, blockInfo)) {
      // Perform one read to get the stream created
      int read = bis.read(readBuffer);
      assertEquals(100, read);

      bis.seek(1024);
      readBuffer.clear();
      resetAndAdvanceDataGenerator(1024);
      bis.read(readBuffer);
      ECStreamTestUtil.assertBufferMatches(readBuffer, dataGenerator);
      assertEquals(1124, bis.getPos());

      // Set the non-reconstruction reader to thrown an exception on seek
      streamFactory.getStreams().get(false).setShouldErrorOnSeek(true);
      bis.seek(2048);
      readBuffer.clear();
      resetAndAdvanceDataGenerator(2048);
      bis.read(readBuffer);
      ECStreamTestUtil.assertBufferMatches(readBuffer, dataGenerator);

      // Finally, set the recon reader to fail on seek.
      streamFactory.getStreams().get(true).setShouldErrorOnSeek(true);
      assertThrows(IOException.class, () -> bis.seek(1024));
    }
  }

  @Test
  public void testPreadByteBufferCapabilityIsFalse() throws IOException {
    int blockLength = 5 * ONEMB;
    generateData(blockLength);

    Map<DatanodeDetails, Integer> dnMap =
        ECStreamTestUtil.createIndexMap(1, 2, 3, 4, 5);
    BlockLocationInfo blockInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, blockLength, dnMap);

    try (ECBlockInputStreamProxy bis = createBISProxy(repConfig, blockInfo)) {
      // The positioned read is the serialized default from ExtendedInputStream, which moves the cursor
      // and puts it back, so the stream must not claim it can pread without moving the cursor.
      assertFalse(bis.hasCapability(StreamCapabilities.PREADBYTEBUFFER));
      assertTrue(bis.hasCapability(StreamCapabilities.READBYTEBUFFER));
      assertTrue(bis.hasCapability(StreamCapabilities.UNBUFFER));
    }
  }

  @Test
  public void testPositionedReadAcrossCellAndStripeBoundaries()
      throws IOException {
    int blockLength = 5 * ONEMB;
    ByteBuffer data = generateData(blockLength);

    Map<DatanodeDetails, Integer> dnMap =
        ECStreamTestUtil.createIndexMap(1, 2, 3, 4, 5);
    BlockLocationInfo blockInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, blockLength, dnMap);

    // {position, length} ranges crossing the cell (ONEMB) and stripe (3 * ONEMB) boundaries.
    long[][] ranges = new long[][] {
        {ONEMB - 512, 1024},
        {3L * ONEMB - 512, 1024},
        {ONEMB - 100, ONEMB + 200},
        {2L * ONEMB, 3L * ONEMB},
    };

    try (ECBlockInputStreamProxy bis = createBISProxy(repConfig, blockInfo)) {
      // Move the cursor off zero so restoring it is actually asserted.
      assertEquals(100, bis.read(ByteBuffer.allocate(100)));
      assertEquals(100, bis.getPos());

      for (long[] range : ranges) {
        long position = range[0];
        int length = (int) range[1];
        byte[] expected = expectedBytes(data, position, length);

        ByteBuffer readBuffer = ByteBuffer.allocate(length);
        assertEquals(length, bis.read(position, readBuffer));
        assertArrayEquals(expected, readBuffer.array());
        assertEquals(100, bis.getPos());

        byte[] readArray = new byte[length];
        assertEquals(length, bis.read(position, readArray, 0, length));
        assertArrayEquals(expected, readArray);
        assertEquals(100, bis.getPos());
      }
    }
  }

  @Test
  public void testPositionedReadFailsOverToReconstruction()
      throws IOException {
    int blockLength = 5 * ONEMB;
    ByteBuffer data = generateData(blockLength);

    Map<DatanodeDetails, Integer> dnMap =
        ECStreamTestUtil.createIndexMap(1, 2, 3, 4, 5);
    BlockLocationInfo blockInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, blockLength, dnMap);
    DatanodeDetails badDN = blockInfo.getPipeline().getFirstNode();

    long position = 3L * ONEMB - 512;
    int length = 1024;
    byte[] expected = expectedBytes(data, position, length);

    try (ECBlockInputStreamProxy bis = createBISProxy(repConfig, blockInfo)) {
      assertEquals(100, bis.read(ByteBuffer.allocate(100)));
      assertEquals(100, bis.getPos());

      // Error 512 bytes into the positioned read, ie. exactly on the stripe boundary, so the proxy has to
      // rewind the buffer and re-read the range from the reconstruction reader inside the pread.
      streamFactory.getStreams().get(false).setShouldError(true,
          (int) position + 512,
          new BadDataLocationException(badDN, "Simulated Error"));

      ByteBuffer readBuffer = ByteBuffer.allocate(length);
      assertEquals(length, bis.read(position, readBuffer));
      assertArrayEquals(expected, readBuffer.array());
      assertEquals(100, bis.getPos());
      assertEquals(badDN, streamFactory.getFailedLocations().get(0));
      assertThat(streamFactory.getStreams()).containsKey(true);

      // Further positioned reads are served by the reconstruction reader and still leave the cursor alone.
      byte[] readArray = new byte[length];
      assertEquals(length, bis.read(position, readArray, 0, length));
      assertArrayEquals(expected, readArray);
      assertEquals(100, bis.getPos());
    }
  }

  @Test
  public void testConcurrentPositionedReads() throws Exception {
    int blockLength = 5 * ONEMB;
    ByteBuffer data = generateData(blockLength);

    Map<DatanodeDetails, Integer> dnMap =
        ECStreamTestUtil.createIndexMap(1, 2, 3, 4, 5);
    BlockLocationInfo blockInfo =
        ECStreamTestUtil.createKeyInfo(repConfig, blockLength, dnMap);

    int threadCount = 8;
    int length = 4096;
    long[] positions = new long[threadCount];
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    try (ECBlockInputStreamProxy bis = createBISProxy(repConfig, blockInfo)) {
      assertEquals(100, bis.read(ByteBuffer.allocate(100)));
      assertEquals(100, bis.getPos());

      CountDownLatch start = new CountDownLatch(1);
      List<Future<byte[]>> futures = new ArrayList<>();
      for (int i = 0; i < threadCount; i++) {
        final int index = i;
        final long position = (long) i * (ONEMB / 2) + ONEMB - 2048;
        positions[i] = position;
        futures.add(executor.submit(() -> {
          start.await();
          if (index % 2 == 0) {
            ByteBuffer readBuffer = ByteBuffer.allocate(length);
            bis.readFully(position, readBuffer);
            return readBuffer.array();
          }
          byte[] readArray = new byte[length];
          bis.readFully(position, readArray);
          return readArray;
        }));
      }
      start.countDown();
      for (int i = 0; i < threadCount; i++) {
        assertArrayEquals(expectedBytes(data, positions[i], length),
            futures.get(i).get());
      }
      assertEquals(100, bis.getPos());
    } finally {
      executor.shutdownNow();
    }
  }

  private static byte[] expectedBytes(ByteBuffer data, long position,
      int length) {
    byte[] expected = new byte[length];
    System.arraycopy(data.array(), (int) position, expected, 0, length);
    return expected;
  }

  private ByteBuffer generateData(int length) {
    ByteBuffer data = ByteBuffer.allocate(length);
    ECStreamTestUtil.randomFill(data, dataGenerator);
    streamFactory.setData(data);
    return data;
  }

  private void resetAndAdvanceDataGenerator(long position) {
    dataGenerator = new SplittableRandom(randomSeed);
    for (long i = 0; i < position; i++) {
      dataGenerator.nextInt(255);
    }
  }

  private ECBlockInputStreamProxy createBISProxy(ECReplicationConfig rConfig,
      BlockLocationInfo blockInfo) {
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    return new ECBlockInputStreamProxy(
        rConfig, blockInfo, null, null, streamFactory,
        clientConfig);
  }

  private static class TestECBlockInputStreamFactory
      implements ECBlockInputStreamFactory {

    private ByteBuffer data;

    private Map<Boolean, ECStreamTestUtil.TestBlockInputStream> streams
        = new HashMap<>();

    private List<DatanodeDetails> failedLocations;

    public void setData(ByteBuffer data) {
      this.data = data;
    }

    public Map<Boolean, ECStreamTestUtil.TestBlockInputStream> getStreams() {
      return streams;
    }

    public List<DatanodeDetails> getFailedLocations() {
      return failedLocations;
    }

    @Override
    public BlockExtendedInputStream create(boolean missingLocations,
        List<DatanodeDetails> failedDatanodes,
        ReplicationConfig repConfig, BlockLocationInfo blockInfo,
        XceiverClientFactory xceiverFactory,
        Function<BlockID, BlockLocationInfo> refreshFunction,
        OzoneClientConfig config) {
      this.failedLocations = failedDatanodes;
      ByteBuffer wrappedBuffer =
          ByteBuffer.wrap(data.array(), 0, data.capacity());
      ECStreamTestUtil.TestBlockInputStream is =
          new ECStreamTestUtil.TestBlockInputStream(blockInfo.getBlockID(),
              blockInfo.getLength(), wrappedBuffer);
      streams.put(missingLocations, is);
      return is;
    }
  }

}
