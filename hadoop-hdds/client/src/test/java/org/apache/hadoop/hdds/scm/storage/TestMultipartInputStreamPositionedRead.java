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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.hdds.client.BlockID;
import org.junit.jupiter.api.Test;

/**
 * Tests the positioned read (pread) of {@link MultipartInputStream} and the serialized default
 * implementation it inherits from {@link ExtendedInputStream}.
 */
class TestMultipartInputStreamPositionedRead {

  private static final String KEY = "key";
  private static final int[] PART_LENGTHS = {10, 25, 7};
  private static final int KEY_LENGTH = 42;

  private static byte[] keyData() {
    byte[] data = new byte[KEY_LENGTH];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) i;
    }
    return data;
  }

  private static List<byte[]> partData() {
    byte[] data = keyData();
    List<byte[]> parts = new ArrayList<>();
    int offset = 0;
    for (int len : PART_LENGTHS) {
      parts.add(Arrays.copyOfRange(data, offset, offset + len));
      offset += len;
    }
    return parts;
  }

  private static MultipartInputStream streamWithSerializedParts() {
    List<SeekableArrayInputStream> parts = new ArrayList<>();
    int i = 0;
    for (byte[] data : partData()) {
      parts.add(new SeekableArrayInputStream(i++, data));
    }
    return new MultipartInputStream(KEY, parts);
  }

  private static List<PositionedArrayInputStream> positionedParts() {
    List<PositionedArrayInputStream> parts = new ArrayList<>();
    int i = 0;
    for (byte[] data : partData()) {
      parts.add(new PositionedArrayInputStream(i++, data));
    }
    return parts;
  }

  @Test
  void testPreadAcrossPartsRecordsPerPartOffsets() throws IOException {
    List<PositionedArrayInputStream> parts = positionedParts();
    try (MultipartInputStream stream = new MultipartInputStream(KEY, parts)) {
      // 35 bytes from position 5 spans all three parts.
      ByteBuffer buf = ByteBuffer.allocate(35);
      assertEquals(35, stream.read(5, buf));
      assertArrayEquals(Arrays.copyOfRange(keyData(), 5, 40), buf.array());

      assertEquals(Collections.singletonList("5,5"), parts.get(0).getReads());
      assertEquals(Collections.singletonList("0,25"), parts.get(1).getReads());
      assertEquals(Collections.singletonList("0,5"), parts.get(2).getReads());

      // The pread must not move the cursor of the multipart stream.
      assertEquals(0, stream.getPos());
    }
  }

  @Test
  void testPreadWithinASinglePart() throws IOException {
    List<PositionedArrayInputStream> parts = positionedParts();
    try (MultipartInputStream stream = new MultipartInputStream(KEY, parts)) {
      ByteBuffer buf = ByteBuffer.allocate(4);
      assertEquals(4, stream.read(12, buf));
      assertArrayEquals(Arrays.copyOfRange(keyData(), 12, 16), buf.array());

      assertTrue(parts.get(0).getReads().isEmpty());
      assertEquals(Collections.singletonList("2,4"), parts.get(1).getReads());
      assertTrue(parts.get(2).getReads().isEmpty());
    }
  }

  @Test
  void testPreadIsClampedAtTheEndOfTheKey() throws IOException {
    List<PositionedArrayInputStream> parts = positionedParts();
    try (MultipartInputStream stream = new MultipartInputStream(KEY, parts)) {
      ByteBuffer buf = ByteBuffer.allocate(20);
      assertEquals(2, stream.read(KEY_LENGTH - 2, buf));
      buf.flip();
      byte[] read = new byte[buf.remaining()];
      buf.get(read);
      assertArrayEquals(Arrays.copyOfRange(keyData(), KEY_LENGTH - 2, KEY_LENGTH), read);
      assertEquals(Collections.singletonList("5,2"), parts.get(2).getReads());
    }
  }

  @Test
  void testPreadAtAndPastEof() throws IOException {
    for (MultipartInputStream stream : streams()) {
      try (MultipartInputStream s = stream) {
        assertEquals(-1, s.read(KEY_LENGTH, ByteBuffer.allocate(8)));
        assertEquals(-1, s.read(KEY_LENGTH + 100, ByteBuffer.allocate(8)));
      }
    }
  }

  @Test
  void testPreadWithEmptyBuffer() throws IOException {
    for (MultipartInputStream stream : streams()) {
      try (MultipartInputStream s = stream) {
        assertEquals(0, s.read(0, ByteBuffer.allocate(0)));
        assertEquals(0, s.read(KEY_LENGTH + 100, ByteBuffer.allocate(0)));
      }
    }
  }

  @Test
  void testPreadWithNegativePosition() throws IOException {
    for (MultipartInputStream stream : streams()) {
      try (MultipartInputStream s = stream) {
        assertEquals(-1, s.read(-1, ByteBuffer.allocate(8)));
        assertEquals(0, s.getPos());
      }
    }
  }

  @Test
  void testReadFullyPastEofThrowsAndLeavesPositionUnchanged() throws IOException {
    for (MultipartInputStream stream : streams()) {
      try (MultipartInputStream s = stream) {
        s.seek(3);
        assertThrows(EOFException.class, () -> s.readFully(KEY_LENGTH - 4, ByteBuffer.allocate(10)));
        assertEquals(3, s.getPos());
      }
    }
  }

  @Test
  void testByteArrayVariants() throws IOException {
    for (MultipartInputStream stream : streams()) {
      try (MultipartInputStream s = stream) {
        byte[] expected = keyData();

        byte[] b = new byte[12];
        assertEquals(12, s.read(8, b, 0, 12));
        assertArrayEquals(Arrays.copyOfRange(expected, 8, 20), b);

        byte[] full = new byte[KEY_LENGTH];
        s.readFully(0, full);
        assertArrayEquals(expected, full);

        byte[] partial = new byte[10];
        s.readFully(30, partial, 2, 8);
        assertArrayEquals(Arrays.copyOfRange(expected, 30, 38), Arrays.copyOfRange(partial, 2, 10));

        assertEquals(0, s.getPos());
      }
    }
  }

  @Test
  void testHasCapabilityIsAggregatedOverParts() throws IOException {
    List<PositionedArrayInputStream> allPositioned = positionedParts();
    try (MultipartInputStream stream = new MultipartInputStream(KEY, allPositioned)) {
      assertTrue(stream.hasCapability(StreamCapabilities.PREADBYTEBUFFER));
      assertTrue(stream.hasCapability(StreamCapabilities.READBYTEBUFFER));
    }

    List<PartInputStream> mixed = new ArrayList<>(positionedParts());
    mixed.set(1, new SeekableArrayInputStream(1, partData().get(1)));
    try (MultipartInputStream stream = new MultipartInputStream(KEY, mixed)) {
      assertFalse(stream.hasCapability(StreamCapabilities.PREADBYTEBUFFER));
      assertTrue(stream.hasCapability(StreamCapabilities.READBYTEBUFFER));
    }

    try (MultipartInputStream stream = streamWithSerializedParts()) {
      assertFalse(stream.hasCapability(StreamCapabilities.PREADBYTEBUFFER));
    }
  }

  @Test
  void testConcurrentPreadsWithSerializedParts() throws Exception {
    final int threads = 8;
    byte[] expected = keyData();
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    try (MultipartInputStream stream = streamWithSerializedParts()) {
      CountDownLatch start = new CountDownLatch(1);
      List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        final int position = t * 4;
        final int len = KEY_LENGTH - position;
        futures.add(executor.submit(() -> {
          start.await();
          for (int i = 0; i < 50; i++) {
            ByteBuffer buf = ByteBuffer.allocate(len);
            int read = stream.read(position, buf);
            assertEquals(len, read);
            assertArrayEquals(Arrays.copyOfRange(expected, position, position + len), buf.array());
          }
          return null;
        }));
      }

      start.countDown();
      // Interleave sequential reads on the same stream while the preads are running.
      for (int i = 0; i < 50; i++) {
        stream.seek(6);
        byte[] sequential = new byte[16];
        int read = stream.read(sequential, 0, sequential.length);
        assertEquals(16, read);
        assertArrayEquals(Arrays.copyOfRange(expected, 6, 22), sequential);
        assertEquals(22, stream.getPos());
      }

      for (Future<?> future : futures) {
        future.get(60, TimeUnit.SECONDS);
      }
      assertEquals(22, stream.getPos());
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void testPreadDoesNotWaitForParkedSequentialRead() throws Exception {
    CountDownLatch parked = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    List<ParkingPart> parts = new ArrayList<>();
    int i = 0;
    for (byte[] data : partData()) {
      parts.add(new ParkingPart(i++, data, parked, release));
    }

    byte[] expected = keyData();
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try (MultipartInputStream stream = new MultipartInputStream(KEY, parts)) {
      stream.initialize();

      byte[] sequential = new byte[16];
      Future<Integer> sequentialRead = executor.submit(() -> stream.read(sequential, 0, sequential.length));
      // The sequential read now holds the monitor of the stream inside readWithStrategy and is parked
      // inside the part, so a pread which takes that monitor would never complete.
      assertTrue(parked.await(10, TimeUnit.SECONDS), "The sequential read never reached the part");

      Future<byte[]> positionedRead = executor.submit(() -> {
        ByteBuffer buffer = ByteBuffer.allocate(15);
        assertEquals(15, stream.read(20, buffer));
        return buffer.array();
      });
      assertArrayEquals(Arrays.copyOfRange(expected, 20, 35), positionedRead.get(10, TimeUnit.SECONDS));

      release.countDown();
      assertEquals(16, sequentialRead.get(10, TimeUnit.SECONDS).intValue());
      assertArrayEquals(Arrays.copyOfRange(expected, 0, 16), sequential);
      assertEquals(16, stream.getPos());
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void testPreadInitializesAnUninitializedStream() throws IOException {
    MutableLengthPart part = new MutableLengthPart(0, keyData(), 4);
    try (MultipartInputStream stream = new MultipartInputStream(KEY, Collections.singletonList(part))) {
      assertEquals(4, stream.getLength());

      // The part grew after the stream was constructed, so only initialize() can pick up the new length.
      part.setLength(KEY_LENGTH);
      ByteBuffer buf = ByteBuffer.allocate(8);
      assertEquals(8, stream.read(0, buf));
      assertArrayEquals(Arrays.copyOfRange(keyData(), 0, 8), buf.array());
      assertEquals(KEY_LENGTH, stream.getLength());
    }
  }

  private static List<MultipartInputStream> streams() {
    return Arrays.asList(streamWithSerializedParts(), new MultipartInputStream(KEY, positionedParts()));
  }

  /**
   * A part backed by an in-memory array which only supports sequential reads and seeks, so positioned
   * reads go through the serialized default of {@link ExtendedInputStream}.
   */
  private static class SeekableArrayInputStream extends BlockExtendedInputStream {

    private final BlockID blockID;
    private final byte[] data;
    private long pos;

    SeekableArrayInputStream(int index, byte[] data) {
      this.blockID = new BlockID(1, index);
      this.data = data;
    }

    byte[] getData() {
      return data;
    }

    @Override
    public BlockID getBlockID() {
      return blockID;
    }

    @Override
    public long getLength() {
      return data.length;
    }

    @Override
    public synchronized long getPos() {
      return pos;
    }

    @Override
    public synchronized void seek(long newPos) throws IOException {
      if (newPos < 0 || newPos > data.length) {
        throw new EOFException("EOF encountered at pos: " + newPos + " for block: " + blockID);
      }
      pos = newPos;
    }

    @Override
    public synchronized int read(byte[] b, int off, int len) throws IOException {
      return read(ByteBuffer.wrap(b, off, len));
    }

    @Override
    public synchronized int read(ByteBuffer dst) {
      if (!dst.hasRemaining()) {
        return 0;
      }
      int numBytesToRead = (int) Math.min(dst.remaining(), data.length - pos);
      if (numBytesToRead <= 0) {
        return EOF;
      }
      dst.put(data, (int) pos, numBytesToRead);
      pos += numBytesToRead;
      return numBytesToRead;
    }

    @Override
    protected int readWithStrategy(ByteReaderStrategy strategy) {
      throw new UnsupportedOperationException("read is overridden directly");
    }

    @Override
    public void unbuffer() {
    }

    @Override
    public void close() {
    }
  }

  /**
   * A part which serves positioned reads directly from the backing array without moving its cursor, and
   * records the (offset, length) of every positioned read it served.
   */
  private static class PositionedArrayInputStream extends SeekableArrayInputStream {

    private final List<String> reads = Collections.synchronizedList(new ArrayList<>());

    PositionedArrayInputStream(int index, byte[] data) {
      super(index, data);
    }

    List<String> getReads() {
      return reads;
    }

    @Override
    public int read(long position, ByteBuffer dst) {
      byte[] data = getData();
      if (!dst.hasRemaining()) {
        return 0;
      }
      if (position < 0 || position >= data.length) {
        return EOF;
      }
      int numBytesToRead = (int) Math.min(dst.remaining(), data.length - position);
      reads.add(position + "," + numBytesToRead);
      dst.put(data, (int) position, numBytesToRead);
      return numBytesToRead;
    }

    @Override
    public boolean hasCapability(String capability) {
      if (StreamCapabilities.PREADBYTEBUFFER.equalsIgnoreCase(capability)) {
        return true;
      }
      return super.hasCapability(capability);
    }
  }

  /**
   * A part whose sequential read parks until it is released, without holding its own monitor over the
   * positioned read, so that only the locking of the MultipartInputStream can serialize the two.
   */
  private static class ParkingPart extends PositionedArrayInputStream {

    private final CountDownLatch parked;
    private final CountDownLatch release;

    ParkingPart(int index, byte[] data, CountDownLatch parked, CountDownLatch release) {
      super(index, data);
      this.parked = parked;
      this.release = release;
    }

    @Override
    public int read(ByteBuffer dst) {
      parked.countDown();
      try {
        assertTrue(release.await(60, TimeUnit.SECONDS), "The sequential read was never released");
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      }
      return super.read(dst);
    }
  }

  /**
   * A part whose reported length can be changed after the MultipartInputStream has been constructed, so
   * that a recomputation of the key length by initialize() becomes observable.
   */
  private static class MutableLengthPart extends PositionedArrayInputStream {

    private volatile long length;

    MutableLengthPart(int index, byte[] data, long length) {
      super(index, data);
      this.length = length;
    }

    void setLength(long newLength) {
      this.length = newLength;
    }

    @Override
    public long getLength() {
      return length;
    }
  }
}
