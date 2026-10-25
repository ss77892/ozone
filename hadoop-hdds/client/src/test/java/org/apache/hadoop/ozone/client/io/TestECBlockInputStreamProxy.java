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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
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
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void positionedReadsRememberFailedLocations(boolean sequentialFirst) throws Exception {
    byte[] data = generateData(1024).array();
    BlockLocationInfo info = ECStreamTestUtil.createKeyInfo(repConfig, data.length,
        ECStreamTestUtil.createIndexMap(1, 2, 3, 4, 5));
    // Two indexes fail at once; all of them are excluded from the retry.
    Map<Integer, DatanodeDetails> bad = new TreeMap<>();
    bad.put(0, info.getPipeline().getNodes().get(0));
    bad.put(1, info.getPipeline().getNodes().get(1));
    AtomicInteger failedAttempts = new AtomicInteger();
    ECBlockInputStreamFactory factory = (missing, failed, replication, block, xceiver, refresh, config) -> {
      if (missing) {
        assertThat(failed).containsExactlyElementsOf(bad.values());
      }
      return new ECStreamTestUtil.TestBlockInputStream(block.getBlockID(), block.getLength(), ByteBuffer.wrap(data)) {
        @Override
        public int read(ByteBuffer destination) throws IOException {
          if (!missing) {
            failedAttempts.incrementAndGet();
            throw new BadDataLocationException(bad, new IOException("failed data replicas"));
          }
          return super.read(destination);
        }

        @Override
        protected int readPositioned(long position, ByteBuffer destination) throws IOException {
          if (!missing) {
            failedAttempts.incrementAndGet();
            throw new BadDataLocationException(bad, new IOException("failed data replicas"));
          }
          return super.readPositioned(position, destination);
        }
      };
    };
    try (ECBlockInputStreamProxy stream = new ECBlockInputStreamProxy(repConfig, info, null, null,
        factory, conf.getObject(OzoneClientConfig.class))) {
      ByteBuffer first = ByteBuffer.allocate(10);
      assertEquals(10, sequentialFirst ? stream.read(first) : stream.read(0, first));
      for (int offset : new int[] {17, 203}) {
        ByteBuffer buffer = ByteBuffer.allocate(20);
        stream.readFully(offset, buffer);
        assertArrayEquals(Arrays.copyOfRange(data, offset, offset + 20), buffer.array());
      }
      assertEquals(1, failedAttempts.get());
      assertEquals(sequentialFirst ? 10 : 0, stream.getPos());
    }
  }

  @Test
  void positionedReadsReuseOneReader() throws IOException {
    byte[] data = generateData(5 * ONEMB).array();
    BlockLocationInfo info = ECStreamTestUtil.createKeyInfo(repConfig, data.length,
        ECStreamTestUtil.createIndexMap(1, 2, 3, 4, 5));
    try (ECBlockInputStreamProxy stream = createBISProxy(repConfig, info)) {
      BlockExtendedInputStream reader = null;
      for (int i = 0; i < 100; i++) {
        int offset = random.nextInt(data.length - 100);
        ByteBuffer buffer = ByteBuffer.allocate(100);
        stream.readFully(offset, buffer);
        assertArrayEquals(Arrays.copyOfRange(data, offset, offset + 100), buffer.array());
        if (reader == null) {
          reader = streamFactory.getStreams().get(false);
        }
      }
      assertThat(streamFactory.getStreams().get(false)).isSameAs(reader);
      assertThat(streamFactory.getStreams()).doesNotContainKey(true);
      assertEquals(0, stream.getPos());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void emptyPositionedReadsDoNotCreateReaders(boolean missing) throws Exception {
    int length = 100;
    generateData(length);
    BlockLocationInfo info = ECStreamTestUtil.createKeyInfo(repConfig, length,
        missing ? ECStreamTestUtil.createIndexMap(2, 3, 4, 5) : ECStreamTestUtil.createIndexMap(1, 2, 3, 4, 5));
    try (ECBlockInputStreamProxy stream = createBISProxy(repConfig, info)) {
      BlockExtendedInputStream reader = streamFactory.getStreams().get(missing);
      ByteBuffer empty = ByteBuffer.allocate(0);
      for (long position : new long[] {0, length, length + 1}) {
        assertEquals(0, stream.readPositioned(position, empty));
        assertEquals(0, stream.read(position, empty));
      }
      assertThat(streamFactory.getStreams().get(missing)).isSameAs(reader);
      assertEquals(-1, stream.readPositioned(length, ByteBuffer.allocate(1)));
      assertEquals(-1, stream.readPositioned(length + 1, ByteBuffer.allocate(1)));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"healthy", "missing", "failover", "failure"})
  void positionedReadsOverlapAndCloseTheirReaders(String mode) throws Exception {
    byte[] data = generateData(1024).array();
    BlockLocationInfo info = ECStreamTestUtil.createKeyInfo(repConfig, data.length,
        "missing".equals(mode) ? ECStreamTestUtil.createIndexMap(2, 3, 4, 5)
            : ECStreamTestUtil.createIndexMap(1, 2, 3, 4, 5));
    CyclicBarrier reads = new CyclicBarrier(2);
    AtomicInteger opened = new AtomicInteger();
    AtomicInteger closed = new AtomicInteger();
    ECBlockInputStreamFactory factory = (missing, failed, replication, block, xceiver, refresh, config) -> {
      opened.incrementAndGet();
      return new ECStreamTestUtil.TestBlockInputStream(block.getBlockID(), block.getLength(), ByteBuffer.wrap(data)) {
        @Override
        public int read(ByteBuffer destination) throws IOException {
          beforeRead(destination);
          return super.read(destination);
        }

        @Override
        protected int readPositioned(long position, ByteBuffer destination) throws IOException {
          beforeRead(destination);
          return super.readPositioned(position, destination);
        }

        private void beforeRead(ByteBuffer destination) throws IOException {
          if ("failover".equals(mode) && !missing) {
            destination.put((byte) 0); // Retry must overwrite bytes from the failed attempt.
            throw new BadDataLocationException(info.getPipeline().getFirstNode(), "failed data replica");
          }
          try {
            reads.await(10, TimeUnit.SECONDS);
          } catch (Exception e) {
            throw new IOException("EC positioned reads did not overlap", e);
          }
          if ("failure".equals(mode)) {
            throw new IOException("injected failure");
          }
        }

        @Override
        public void close() {
          closed.incrementAndGet();
        }
      };
    };
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try (ECBlockInputStreamProxy stream = new ECBlockInputStreamProxy(repConfig, info, null, null,
        factory, conf.getObject(OzoneClientConfig.class))) {
      stream.seek(71);
      List<Future<?>> futures = new ArrayList<>();
      for (int offset : new int[] {17, 203}) {
        futures.add(pool.submit((Callable<Void>) () -> {
          ByteBuffer destination = ByteBuffer.allocateDirect(333);
          if ("failure".equals(mode)) {
            assertThrows(IOException.class, () -> stream.readFully(offset, destination));
          } else {
            stream.readFully(offset, destination);
            destination.flip();
            byte[] actual = new byte[destination.remaining()];
            destination.get(actual);
            assertArrayEquals(Arrays.copyOfRange(data, offset, offset + 333), actual);
          }
          return null;
        }));
      }
      for (Future<?> future : futures) {
        future.get(20, TimeUnit.SECONDS);
      }
      assertEquals(71, stream.getPos());
      // The sequential reader stays open, and so does the positioned reader unless locations are missing or a
      // location failed, as positioned reads then no longer use it.
      int longLived = "missing".equals(mode) || "failover".equals(mode) ? 1 : 2;
      if ("healthy".equals(mode)) {
        assertEquals(longLived, opened.get());
      }
      assertEquals(opened.get() - longLived, closed.get());
      assertEquals(-1, stream.read(data.length, ByteBuffer.allocate(1)));
    } finally {
      pool.shutdownNow();
    }
    assertEquals(opened.get(), closed.get());
  }

  @ParameterizedTest
  @ValueSource(strings = {"positioned", "sequential"})
  void positionedReaderIsNotClosedUnderAPositionedRead(String failingRead) throws Exception {
    byte[] data = generateData(1024).array();
    BlockLocationInfo info = ECStreamTestUtil.createKeyInfo(repConfig, data.length,
        ECStreamTestUtil.createIndexMap(1, 2, 3, 4, 5));
    CountDownLatch readStarted = new CountDownLatch(1);
    CountDownLatch readReleased = new CountDownLatch(1);
    // The readers over the data locations: the sequential reader, then the positioned reader.
    List<ECStreamTestUtil.TestBlockInputStream> dataReaders = new CopyOnWriteArrayList<>();
    AtomicInteger reconstructionReaders = new AtomicInteger();
    ECBlockInputStreamFactory factory = (missing, failed, replication, block, xceiver, refresh, config) -> {
      ECStreamTestUtil.TestBlockInputStream reader =
          new ECStreamTestUtil.TestBlockInputStream(block.getBlockID(), block.getLength(), ByteBuffer.wrap(data)) {
            @Override
            public int read(ByteBuffer destination) throws IOException {
              if (!missing && "sequential".equals(failingRead)) {
                throw new BadDataLocationException(info.getPipeline().getFirstNode(), "failed data replica");
              }
              return super.read(destination);
            }

            @Override
            protected int readPositioned(long position, ByteBuffer destination) throws IOException {
              if (!missing) {
                if (position != 17) {
                  throw new BadDataLocationException(info.getPipeline().getFirstNode(), "failed data replica");
                }
                readStarted.countDown();
                try {
                  if (!readReleased.await(10, TimeUnit.SECONDS)) {
                    throw new IOException("The positioned read was not released");
                  }
                } catch (InterruptedException e) {
                  throw new IOException(e);
                }
              }
              // fails if the reader has been closed under the read
              return super.readPositioned(position, destination);
            }
          };
      if (missing) {
        reconstructionReaders.incrementAndGet();
      } else {
        dataReaders.add(reader);
      }
      return reader;
    };
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try (ECBlockInputStreamProxy stream = new ECBlockInputStreamProxy(repConfig, info, null, null, factory,
        conf.getObject(OzoneClientConfig.class))) {
      ByteBuffer blocked = ByteBuffer.allocate(100);
      Future<?> blockedRead = pool.submit((Callable<Void>) () -> {
        stream.readFully(17, blocked);
        return null;
      });
      assertTrue(readStarted.await(10, TimeUnit.SECONDS));
      assertEquals(2, dataReaders.size());
      ECStreamTestUtil.TestBlockInputStream positionedReader = dataReaders.get(1);

      // A location fails while the other positioned read is inside the positioned reader.
      ByteBuffer buffer = ByteBuffer.allocate(100);
      if ("sequential".equals(failingRead)) {
        assertEquals(100, stream.read(buffer));
        assertArrayEquals(Arrays.copyOfRange(data, 0, 100), buffer.array());
      } else {
        stream.readFully(203, buffer);
        assertArrayEquals(Arrays.copyOfRange(data, 203, 303), buffer.array());
      }
      assertEquals(1, reconstructionReaders.get());
      assertFalse(positionedReader.isClosed());

      readReleased.countDown();
      blockedRead.get(20, TimeUnit.SECONDS);
      assertArrayEquals(Arrays.copyOfRange(data, 17, 117), blocked.array());
      // Closed by the last positioned read to leave it; every later positioned read is a reconstruction read.
      assertTrue(positionedReader.isClosed());
      stream.readFully(17, ByteBuffer.allocate(100));
      assertEquals(2, dataReaders.size());
      assertEquals(2, reconstructionReaders.get());
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void unbufferReachesThePositionedReader() throws IOException {
    byte[] data = generateData(1024).array();
    BlockLocationInfo info = ECStreamTestUtil.createKeyInfo(repConfig, data.length,
        ECStreamTestUtil.createIndexMap(1, 2, 3, 4, 5));
    // The sequential reader, then the positioned reader.
    List<ECStreamTestUtil.TestBlockInputStream> readers = new ArrayList<>();
    ECBlockInputStreamFactory factory = (missing, failed, replication, block, xceiver, refresh, config) -> {
      ECStreamTestUtil.TestBlockInputStream reader =
          new ECStreamTestUtil.TestBlockInputStream(block.getBlockID(), block.getLength(), ByteBuffer.wrap(data));
      readers.add(reader);
      return reader;
    };
    try (ECBlockInputStreamProxy stream = new ECBlockInputStreamProxy(repConfig, info, null, null, factory,
        conf.getObject(OzoneClientConfig.class))) {
      // Without a positioned read there is no positioned reader to unbuffer, and unbuffer does not create one.
      stream.unbuffer();
      assertEquals(1, readers.size());
      assertEquals(1, readers.get(0).getUnbufferCalls());

      ByteBuffer buffer = ByteBuffer.allocate(100);
      stream.readFully(17, buffer);
      assertArrayEquals(Arrays.copyOfRange(data, 17, 117), buffer.array());
      assertEquals(2, readers.size());
      // The positioned reader is unbuffered and kept, like the sequential reader, so that the next positioned read does
      // not repeat the GetBlock of each index.
      stream.unbuffer();
      assertEquals(2, readers.get(0).getUnbufferCalls());
      assertEquals(1, readers.get(1).getUnbufferCalls());
      assertFalse(readers.get(1).isClosed());

      // The next positioned read reuses it.
      buffer.clear();
      stream.readFully(200, buffer);
      assertArrayEquals(Arrays.copyOfRange(data, 200, 300), buffer.array());
      assertEquals(2, readers.size());
      assertFalse(readers.get(1).isClosed());
    }
  }

  @Test
  @Timeout(30)
  void unbufferHoldsNoMonitorWhileWaitingOnAPositionedRead() throws Exception {
    byte[] data = generateData(1024).array();
    BlockLocationInfo info = ECStreamTestUtil.createKeyInfo(repConfig, data.length,
        ECStreamTestUtil.createIndexMap(1, 2, 3, 4, 5));
    CountDownLatch readStarted = new CountDownLatch(1);
    CountDownLatch readReleased = new CountDownLatch(1);
    // The sequential reader, then the positioned reader.
    List<ECStreamTestUtil.TestBlockInputStream> readers = new CopyOnWriteArrayList<>();
    ECBlockInputStreamFactory factory = (missing, failed, replication, block, xceiver, refresh, config) -> {
      ECStreamTestUtil.TestBlockInputStream reader =
          new ECStreamTestUtil.TestBlockInputStream(block.getBlockID(), block.getLength(), ByteBuffer.wrap(data)) {
            @Override
            protected int readPositioned(long position, ByteBuffer destination) throws IOException {
              // like BlockInputStream.initialize(), a slow positioned read holds the monitor that unbuffer() needs
              synchronized (this) {
                readStarted.countDown();
                try {
                  if (!readReleased.await(10, TimeUnit.SECONDS)) {
                    throw new IOException("The positioned read was not released");
                  }
                } catch (InterruptedException e) {
                  throw new IOException(e);
                }
                return super.readPositioned(position, destination);
              }
            }
          };
      readers.add(reader);
      return reader;
    };
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try (ECBlockInputStreamProxy stream = new ECBlockInputStreamProxy(repConfig, info, null, null, factory,
        conf.getObject(OzoneClientConfig.class))) {
      ByteBuffer blocked = ByteBuffer.allocate(100);
      Future<?> blockedRead = pool.submit((Callable<Void>) () -> {
        stream.readFully(17, blocked);
        return null;
      });
      assertTrue(readStarted.await(10, TimeUnit.SECONDS));
      assertEquals(2, readers.size());

      // unbuffer() unbuffers the sequential reader, then waits for the positioned read to leave the positioned reader
      // without holding the stream's monitor, so a sequential read completes meanwhile.
      Future<?> unbuffer = pool.submit(stream::unbuffer);
      while (readers.get(0).getUnbufferCalls() == 0) {
        Thread.sleep(1);
      }
      ByteBuffer buffer = ByteBuffer.allocate(100);
      assertEquals(100, stream.read(buffer));
      assertArrayEquals(Arrays.copyOfRange(data, 0, 100), buffer.array());
      assertEquals(100, stream.getPos());
      assertFalse(unbuffer.isDone());

      readReleased.countDown();
      unbuffer.get(10, TimeUnit.SECONDS);
      blockedRead.get(10, TimeUnit.SECONDS);
      assertArrayEquals(Arrays.copyOfRange(data, 17, 117), blocked.array());
      // With a positioned read inside it, the positioned reader is unbuffered and kept, not closed under the read.
      assertEquals(1, readers.get(1).getUnbufferCalls());
      assertFalse(readers.get(1).isClosed());
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void closeDuringPositionedReadIsNotAFailover() throws Exception {
    byte[] data = generateData(1024).array();
    BlockLocationInfo info = ECStreamTestUtil.createKeyInfo(repConfig, data.length,
        ECStreamTestUtil.createIndexMap(1, 2, 3, 4, 5));
    CountDownLatch readStarted = new CountDownLatch(1);
    CountDownLatch streamClosed = new CountDownLatch(1);
    AtomicInteger opened = new AtomicInteger();
    AtomicReference<List<DatanodeDetails>> failedLocations = new AtomicReference<>();
    ECBlockInputStreamFactory factory = (missing, failed, replication, block, xceiver, refresh, config) -> {
      opened.incrementAndGet();
      failedLocations.set(failed);
      return new ECStreamTestUtil.TestBlockInputStream(block.getBlockID(), block.getLength(), ByteBuffer.wrap(data)) {
        @Override
        protected int readPositioned(long position, ByteBuffer destination) throws IOException {
          readStarted.countDown();
          try {
            streamClosed.await(10, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            throw new IOException(e);
          }
          // a location that fails once the proxy has been closed under the read
          throw new BadDataLocationException(info.getPipeline().getFirstNode(), new IOException("closed under read"));
        }
      };
    };
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      ECBlockInputStreamProxy stream = new ECBlockInputStreamProxy(repConfig, info, null, null, factory,
          conf.getObject(OzoneClientConfig.class));
      Future<IOException> failure = pool.submit(
          () -> assertThrows(IOException.class, () -> stream.read(17, ByteBuffer.allocate(100))));
      assertTrue(readStarted.await(10, TimeUnit.SECONDS));
      stream.close();
      streamClosed.countDown();
      // The read fails as closed; the location is not recorded as failed and no reconstruction reader is created.
      IOException e = failure.get(20, TimeUnit.SECONDS);
      assertThat(e.getMessage()).contains("closed");
      assertThat(failedLocations.get()).isEmpty();
      assertEquals(2, opened.get());
    } finally {
      pool.shutdownNow();
    }
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
