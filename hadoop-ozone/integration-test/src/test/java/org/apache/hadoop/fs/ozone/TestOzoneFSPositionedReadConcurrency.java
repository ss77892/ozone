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

package org.apache.hadoop.fs.ozone;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_SCHEME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.ClientConfigForTesting;
import org.apache.hadoop.ozone.DataTestUtil;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Positioned reads through o3fs, run concurrently with a sequential scan of the same stream.
 * <p>
 * The same file (30 MB, spanning several 4 MB blocks and 1 MB chunks) is written to a Ratis bucket and to an
 * EC (RS-3-2-1024k) bucket.  The Ratis cases are run with both read transports, selected by
 * {@code ozone.client.stream.readblock.enable}.
 */
public class TestOzoneFSPositionedReadConcurrency {

  private static final int FILE_LEN = 30 << 20;
  private static final int BLOCK_SIZE = 4 << 20;
  private static final int CHUNK_SIZE = 1 << 20;
  private static final int MAX_PREAD_LEN = 3 << 20;

  private static final int READER_THREADS = 16;
  private static final int PREADS_PER_THREAD = 200;
  private static final int JOIN_TIMEOUT_MINUTES = 10;

  private static final String READ_BLOCK_ENABLE_KEY = "ozone.client.stream.readblock.enable";
  private static final String PREAD_BYTE_BUFFER = "in:preadbytebuffer";

  private static final Path FILE = new Path("/positionedReadFile");

  private static MiniOzoneCluster cluster;
  private static OzoneClient client;
  private static byte[] data;
  private static URI ratisUri;
  private static URI ecUri;

  @BeforeAll
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    // Small blocks and chunks so that a 30 MB file spans several of each.
    ClientConfigForTesting.newBuilder(StorageUnit.BYTES)
        .setBlockSize(BLOCK_SIZE)
        .setChunkSize(CHUNK_SIZE)
        .applyTo(conf);

    // EC RS-3-2 needs 5 datanodes.
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(5).build();
    cluster.waitForClusterToBeReady();
    // Each case opens its own FileSystem with a different client config, so the cache must not hand back another.
    cluster.getConf().setBoolean(String.format("fs.%s.impl.disable.cache", OZONE_URI_SCHEME), true);
    client = cluster.newClient();

    data = new byte[FILE_LEN];
    ThreadLocalRandom.current().nextBytes(data);

    OzoneBucket bucket = DataTestUtil.createVolumeAndBucket(client);
    ratisUri = uriOf(bucket.getVolumeName(), bucket.getName());
    writeFile(ratisUri);

    // EC bucket, built like the one in TestOzoneFSInputStream: RS-3-2 with a 1 MB cell, FSO layout.
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setStorageType(StorageType.DISK);
    builder.setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED);
    builder.setDefaultReplicationConfig(new DefaultReplicationConfig(
        new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS, (int) OzoneConsts.MB)));
    String ecBucket = UUID.randomUUID().toString();
    DataTestUtil.createBucket(client, bucket.getVolumeName(), builder.build(), ecBucket);
    ecUri = uriOf(bucket.getVolumeName(), ecBucket);
    writeFile(ecUri);
  }

  @AfterAll
  public static void teardown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private static URI uriOf(String volume, String bucket) {
    return URI.create(String.format("%s://%s.%s/", OZONE_URI_SCHEME, bucket, volume));
  }

  private static void writeFile(URI uri) throws IOException {
    try (FileSystem fs = FileSystem.get(uri, cluster.getConf());
        FSDataOutputStream out = fs.create(FILE)) {
      out.write(data);
    }
  }

  private static FileSystem ratisFs(boolean isStreamEnable) throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration(cluster.getConf());
    conf.setBoolean(READ_BLOCK_ENABLE_KEY, isStreamEnable);
    return FileSystem.get(ratisUri, conf);
  }

  private static FileSystem ecFs() throws IOException {
    return FileSystem.get(ecUri, new OzoneConfiguration(cluster.getConf()));
  }

  // (a) concurrent positioned reads on a Ratis file, for both read transports

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testConcurrentPositionedReadsRatis(boolean isStreamEnable) throws Exception {
    try (FileSystem fs = ratisFs(isStreamEnable)) {
      runConcurrentPositionedReads(fs);
    }
  }

  // (b) the same on the EC file

  @Test
  public void testConcurrentPositionedReadsEC() throws Exception {
    try (FileSystem fs = ecFs()) {
      runConcurrentPositionedReads(fs);
    }
  }

  // (c) positioned reads around unbuffer(), seek() and the end of the file

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testPositionedReadEdgeCasesRatis(boolean isStreamEnable) throws Exception {
    try (FileSystem fs = ratisFs(isStreamEnable)) {
      runPositionedReadEdgeCases(fs);
    }
  }

  @Test
  public void testPositionedReadEdgeCasesEC() throws Exception {
    try (FileSystem fs = ecFs()) {
      runPositionedReadEdgeCases(fs);
    }
  }

  // (d) the positioned-read-into-ByteBuffer capability is advertised only where the cursor really stays put

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testPreadByteBufferCapabilityRatis(boolean isStreamEnable) throws Exception {
    try (FileSystem fs = ratisFs(isStreamEnable);
        FSDataInputStream in = fs.open(FILE)) {
      assertInstanceOf(OzoneFSInputStream.class, in.getWrappedStream());
      assertTrue(in.hasCapability(PREAD_BYTE_BUFFER),
          "Ratis stream should support positioned reads without moving the cursor");
    }
  }

  @Test
  public void testPreadByteBufferCapabilityEC() throws Exception {
    try (FileSystem fs = ecFs();
        FSDataInputStream in = fs.open(FILE)) {
      assertInstanceOf(OzoneFSInputStream.class, in.getWrappedStream());
      assertFalse(in.hasCapability(PREAD_BYTE_BUFFER),
          "EC positioned reads are serialized against the cursor, so the capability must not be advertised");
    }
  }

  /**
   * Hammers one shared stream with positioned reads from {@link #READER_THREADS} threads while another thread
   * scans the very same stream sequentially.  Every byte read either way is verified against {@link #data},
   * and the scanner checks after each of its own reads that the positioned reads left its cursor alone.
   */
  private void runConcurrentPositionedReads(FileSystem fs) throws Exception {
    final List<Throwable> failures = new CopyOnWriteArrayList<>();
    final CountDownLatch startGate = new CountDownLatch(1);
    final CountDownLatch preadsDone = new CountDownLatch(READER_THREADS);
    final AtomicBoolean keepScanning = new AtomicBoolean(true);
    final ExecutorService executor = Executors.newFixedThreadPool(READER_THREADS + 1);

    try (FSDataInputStream in = fs.open(FILE)) {
      for (int i = 0; i < READER_THREADS; i++) {
        final int threadIndex = i;
        executor.submit(() -> {
          try {
            startGate.await();
            preadWorker(in, threadIndex);
          } catch (Throwable t) {
            failures.add(t);
          } finally {
            preadsDone.countDown();
          }
        });
      }
      executor.submit(() -> {
        try {
          startGate.await();
          // Scan the whole file at least once, and keep scanning while the positioned reads are in flight.
          do {
            sequentialScan(in);
          } while (keepScanning.get());
        } catch (Throwable t) {
          failures.add(t);
        }
      });

      startGate.countDown();
      assertTrue(preadsDone.await(JOIN_TIMEOUT_MINUTES, TimeUnit.MINUTES), "positioned reads did not finish in time");
      keepScanning.set(false);
      executor.shutdown();
      assertTrue(executor.awaitTermination(JOIN_TIMEOUT_MINUTES, TimeUnit.MINUTES),
          "sequential scan did not finish in time");
    } finally {
      executor.shutdownNow();
    }

    if (!failures.isEmpty()) {
      AssertionError error = new AssertionError(failures.size() + " reader thread(s) failed, first: "
          + failures.get(0));
      for (Throwable t : failures) {
        error.addSuppressed(t);
      }
      throw error;
    }
  }

  private void preadWorker(FSDataInputStream in, int threadIndex) throws IOException {
    final ThreadLocalRandom rnd = ThreadLocalRandom.current();
    final byte[] array = new byte[MAX_PREAD_LEN];
    final ByteBuffer heapBuffer = ByteBuffer.wrap(array);
    final ByteBuffer directBuffer = ByteBuffer.allocateDirect(MAX_PREAD_LEN);

    for (int i = 0; i < PREADS_PER_THREAD; i++) {
      final int length = randomLength(rnd);
      final long position = randomPosition(rnd, length);
      // Half of the reads go through read(long, ByteBuffer) (heap and direct), half through
      // read(long, byte[], int, int).
      if (((threadIndex + i) & 1) == 0) {
        preadIntoBuffer(in, position, length, (i & 2) == 0 ? heapBuffer : directBuffer);
      } else {
        preadIntoArray(in, position, length, array);
      }
    }
  }

  private void preadIntoBuffer(FSDataInputStream in, long position, int length, ByteBuffer buffer)
      throws IOException {
    buffer.clear();
    buffer.limit(length);
    int done = 0;
    while (buffer.hasRemaining()) {
      int read = in.read(position + done, buffer);
      assertTrue(read > 0, "read(" + (position + done) + ", ByteBuffer) returned " + read);
      done += read;
    }
    assertEquals(length, done);
    buffer.flip();
    for (int i = 0; i < length; i++) {
      byte actual = buffer.get(i);
      if (actual != data[(int) position + i]) {
        fail("mismatch at " + (position + i) + " of pread(" + position + ", " + length + "): expected "
            + data[(int) position + i] + " but was " + actual);
      }
    }
  }

  private void preadIntoArray(FSDataInputStream in, long position, int length, byte[] array) throws IOException {
    int done = 0;
    while (done < length) {
      int read = in.read(position + done, array, done, length - done);
      assertTrue(read > 0, "read(" + (position + done) + ", byte[], ...) returned " + read);
      done += read;
    }
    assertEquals(length, done);
    assertBytesEqual(position, array, 0, length);
  }

  /**
   * Reads the whole stream sequentially from the beginning, checking the bytes and the cursor as it goes.
   */
  private void sequentialScan(FSDataInputStream in) throws IOException {
    final byte[] buffer = new byte[64 << 10];
    in.seek(0);
    assertEquals(0, in.getPos());
    int offset = 0;
    while (offset < FILE_LEN) {
      int read = in.read(buffer, 0, buffer.length);
      assertTrue(read > 0, "sequential read at " + offset + " returned " + read);
      assertBytesEqual(offset, buffer, 0, read);
      offset += read;
      // Positioned reads from the other threads must not have moved the cursor of the shared stream.
      assertEquals(offset, in.getPos(), "cursor moved during sequential scan");
    }
    assertEquals(FILE_LEN, offset);
    assertEquals(FILE_LEN, in.getPos());
    assertEquals(-1, in.read(buffer, 0, buffer.length));
  }

  private void runPositionedReadEdgeCases(FileSystem fs) throws IOException {
    try (FSDataInputStream in = fs.open(FILE)) {
      // A positioned read right after unbuffer() still returns the right bytes and leaves the cursor at 0.
      in.unbuffer();
      assertPread(in, CHUNK_SIZE - 7, 4096);
      assertEquals(0, in.getPos());

      // After a sequential seek to a mid-file offset, positioned reads leave the cursor at the seeked offset.
      final long seekPos = BLOCK_SIZE + CHUNK_SIZE + 12345;
      in.seek(seekPos);
      assertPread(in, 3L * BLOCK_SIZE - 5, 2 * CHUNK_SIZE);
      assertEquals(seekPos, in.getPos());

      // ... and the sequential read continues from there.
      byte[] sequential = new byte[8192];
      in.readFully(sequential);
      assertBytesEqual(seekPos, sequential, 0, sequential.length);
      assertEquals(seekPos + sequential.length, in.getPos());

      in.unbuffer();
      assertEquals(seekPos + sequential.length, in.getPos());
      assertPread(in, FILE_LEN - 1024, 1024);
      final long cursor = seekPos + sequential.length;
      assertEquals(cursor, in.getPos());

      // readFully(long, ByteBuffer) which runs off the end of the file fails without moving the cursor.
      ByteBuffer oversized = ByteBuffer.allocate(1024);
      assertThrows(EOFException.class, () -> in.readFully(FILE_LEN - 1L, oversized));
      assertEquals(cursor, in.getPos());

      // read(long, ByteBuffer) outside the file returns -1, again without moving the cursor.
      ByteBuffer small = ByteBuffer.allocate(16);
      assertEquals(-1, in.read(-1L, small));
      assertEquals(cursor, in.getPos());
      small.clear();
      assertEquals(-1, in.read(FILE_LEN, small));
      assertEquals(cursor, in.getPos());
      small.clear();
      assertEquals(-1, in.read(FILE_LEN + 1L, small));
      assertEquals(cursor, in.getPos());
    }
  }

  private void assertPread(FSDataInputStream in, long position, int length) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(length);
    in.readFully(position, buffer);
    assertFalse(buffer.hasRemaining());
    assertBytesEqual(position, buffer.array(), 0, length);
  }

  private static void assertBytesEqual(long position, byte[] actual, int off, int len) {
    for (int i = 0; i < len; i++) {
      if (actual[off + i] != data[(int) position + i]) {
        fail("mismatch at " + (position + i) + ": expected " + data[(int) position + i]
            + " but was " + actual[off + i]);
      }
    }
  }

  /**
   * Lengths from a single byte up to 3 MB, weighted towards the small end to keep the runtime modest while
   * still reading across chunk (1 MB) and block (4 MB) boundaries.
   */
  private static int randomLength(ThreadLocalRandom rnd) {
    int bucket = rnd.nextInt(100);
    if (bucket < 80) {
      return rnd.nextInt(1, 32 << 10);
    } else if (bucket < 95) {
      return rnd.nextInt(32 << 10, 512 << 10);
    }
    return rnd.nextInt(1 << 20, MAX_PREAD_LEN + 1);
  }

  /**
   * Positions are either uniformly random, or picked so that the read straddles a chunk or a block boundary.
   */
  private static long randomPosition(ThreadLocalRandom rnd, int length) {
    final long last = FILE_LEN - length;
    final int mode = rnd.nextInt(3);
    if (mode == 0) {
      return rnd.nextLong(0, last + 1);
    }
    final int unit = mode == 1 ? CHUNK_SIZE : BLOCK_SIZE;
    final long boundary = (long) unit * rnd.nextInt(1, FILE_LEN / unit);
    final long back = rnd.nextLong(1, Math.min(length, boundary) + 1);
    return Math.min(Math.max(boundary - back, 0), last);
  }
}
