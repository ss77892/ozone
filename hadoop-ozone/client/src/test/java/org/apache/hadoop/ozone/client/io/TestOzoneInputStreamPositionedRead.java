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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoCodec;
import org.apache.hadoop.crypto.CryptoOutputStream;
import org.apache.hadoop.crypto.CryptoStreamUtils;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.scm.storage.MultipartInputStream;
import org.junit.jupiter.api.Test;

/**
 * Tests the positioned read (pread) delegation of {@link OzoneInputStream} and the serialized
 * positioned read of {@link OzoneCryptoInputStream}.
 */
class TestOzoneInputStreamPositionedRead {

  private static final String KEY = "key";
  private static final Random RANDOM = new Random();

  /** Crypto buffer size used by {@link OzoneCryptoInputStream}, i.e. hadoop.security.crypto.buffer.size. */
  private static final int CRYPTO_BUFFER_SIZE =
      CryptoStreamUtils.getBufferSize(new Configuration());

  private static byte[] randomBytes(int length) {
    byte[] data = new byte[length];
    RANDOM.nextBytes(data);
    return data;
  }

  private static byte[] concat(byte[] first, byte[] second) {
    byte[] data = new byte[first.length + second.length];
    System.arraycopy(first, 0, data, 0, first.length);
    System.arraycopy(second, 0, data, first.length, second.length);
    return data;
  }

  private static KeyInputStream keyInputStream(byte[] data) {
    return new KeyInputStream(KEY, Collections.singletonList(
        new ECStreamTestUtil.TestBlockInputStream(new BlockID(1, 1), data.length, ByteBuffer.wrap(data))));
  }

  private static byte[] encrypt(byte[] plainText, byte[] key, byte[] iv) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (CryptoOutputStream cos = new CryptoOutputStream(out,
        CryptoCodec.getInstance(new Configuration()), key, iv)) {
      cos.write(plainText);
    }
    return out.toByteArray();
  }

  /**
   * Creates a part stream as RpcClient does for an encrypted multipart key: the encrypted part data is
   * served by a KeyInputStream which the OzoneCryptoInputStream decrypts.
   */
  private static OzoneCryptoInputStream cryptoInputStream(byte[] plainText, byte[] key, byte[] iv,
      int partIndex) throws IOException {
    KeyInputStream kis = keyInputStream(encrypt(plainText, key, iv));
    return new OzoneCryptoInputStream(new LengthInputStream(kis, kis.getLength()),
        CryptoCodec.getInstance(new Configuration()), key, iv, KEY, partIndex);
  }

  @Test
  void testPositionedReadDelegatedToKeyInputStream() throws IOException {
    byte[] data = randomBytes(3000);
    try (OzoneInputStream stream = new OzoneInputStream(keyInputStream(data))) {
      // Move the cursor to verify that the positioned reads do not move it.
      stream.seek(100);

      ByteBuffer buf = ByteBuffer.allocate(500);
      assertEquals(500, stream.read(1000, buf));
      assertArrayEquals(Arrays.copyOfRange(data, 1000, 1500), buf.array());
      assertEquals(100, stream.getPos());

      byte[] bytes = new byte[600];
      assertEquals(500, stream.read(700, bytes, 50, 500));
      assertArrayEquals(Arrays.copyOfRange(data, 700, 1200), Arrays.copyOfRange(bytes, 50, 550));
      assertEquals(100, stream.getPos());

      buf = ByteBuffer.allocate(300);
      stream.readFully(2000, buf);
      assertArrayEquals(Arrays.copyOfRange(data, 2000, 2300), buf.array());
      assertEquals(100, stream.getPos());

      bytes = new byte[400];
      stream.readFully(1500, bytes, 100, 300);
      assertArrayEquals(Arrays.copyOfRange(data, 1500, 1800), Arrays.copyOfRange(bytes, 100, 400));

      bytes = new byte[250];
      stream.readFully(2750, bytes);
      assertArrayEquals(Arrays.copyOfRange(data, 2750, 3000), bytes);
      assertEquals(100, stream.getPos());

      // Reads at and past the end of the key.
      assertEquals(-1, stream.read(3000, ByteBuffer.allocate(10)));
      assertEquals(0, stream.read(10, ByteBuffer.allocate(0)));
      assertThrows(EOFException.class, () -> stream.readFully(2990, new byte[20]));
      assertEquals(100, stream.getPos());

      // The parts only support the serialized positioned read.
      assertFalse(stream.hasCapability(StreamCapabilities.PREADBYTEBUFFER));
    }
  }

  @Test
  void testPositionedReadNotSupportedByWrappedStream() throws IOException {
    byte[] data = randomBytes(100);
    try (OzoneInputStream stream = new OzoneInputStream(new ByteArrayInputStream(data))) {
      assertThrows(UnsupportedOperationException.class, () -> stream.read(0, ByteBuffer.allocate(10)));
      assertThrows(UnsupportedOperationException.class, () -> stream.readFully(0, ByteBuffer.allocate(10)));
      assertThrows(UnsupportedOperationException.class, () -> stream.read(0, new byte[10], 0, 10));
      assertThrows(UnsupportedOperationException.class, () -> stream.readFully(0, new byte[10], 0, 10));
      assertThrows(UnsupportedOperationException.class, () -> stream.readFully(0, new byte[10]));
      assertFalse(stream.hasCapability(StreamCapabilities.PREADBYTEBUFFER));
      assertFalse(stream.hasCapability(StreamCapabilities.READBYTEBUFFER));

      // The sequential read is unaffected.
      byte[] read = new byte[100];
      assertEquals(100, stream.read(read, 0, 100));
      assertArrayEquals(data, read);
    }
  }

  @Test
  void testCryptoPositionedRead() throws IOException {
    // A few crypto buffers long, with a length which is not a multiple of the crypto buffer size.
    byte[] plainText = randomBytes(3 * CRYPTO_BUFFER_SIZE + 1234);
    byte[] key = randomBytes(16);
    byte[] iv = randomBytes(16);

    try (OzoneCryptoInputStream stream = cryptoInputStream(plainText, key, iv, 0)) {
      assertFalse(stream.hasCapability(StreamCapabilities.PREADBYTEBUFFER));
      assertEquals(plainText.length, stream.getLength());

      // Read some data sequentially first so that the cursor is at a non zero, unaligned position.
      byte[] sequential = new byte[777];
      assertEquals(777, stream.read(sequential, 0, 777));
      assertArrayEquals(Arrays.copyOfRange(plainText, 0, 777), sequential);
      assertEquals(777, stream.getPos());

      // Positions and lengths which are not aligned with the crypto buffer boundaries.
      int[][] ranges = {
          {0, 10},
          {1, CRYPTO_BUFFER_SIZE},
          {CRYPTO_BUFFER_SIZE - 5, 100},
          {CRYPTO_BUFFER_SIZE + 7, 2 * CRYPTO_BUFFER_SIZE + 3},
          {plainText.length - 10, 10},
      };
      for (int[] range : ranges) {
        int position = range[0];
        int length = range[1];

        ByteBuffer buf = ByteBuffer.allocate(length);
        assertEquals(length, stream.read(position, buf));
        assertArrayEquals(Arrays.copyOfRange(plainText, position, position + length), buf.array());
        assertEquals(777, stream.getPos());

        byte[] bytes = new byte[length];
        stream.readFully(position, bytes);
        assertArrayEquals(Arrays.copyOfRange(plainText, position, position + length), bytes);
        assertEquals(777, stream.getPos());

        bytes = new byte[length + 20];
        assertEquals(length, stream.read(position, bytes, 20, length));
        assertArrayEquals(Arrays.copyOfRange(plainText, position, position + length),
            Arrays.copyOfRange(bytes, 20, length + 20));
        stream.readFully(position, bytes, 20, length);
        assertArrayEquals(Arrays.copyOfRange(plainText, position, position + length),
            Arrays.copyOfRange(bytes, 20, length + 20));
        assertEquals(777, stream.getPos());
      }

      // A read which would go past the end of the part is clamped to the part length.
      ByteBuffer buf = ByteBuffer.allocate(100);
      assertEquals(50, stream.read(plainText.length - 50, buf));
      buf.flip();
      byte[] tail = new byte[50];
      buf.get(tail);
      assertArrayEquals(Arrays.copyOfRange(plainText, plainText.length - 50, plainText.length), tail);

      assertEquals(-1, stream.read(plainText.length, ByteBuffer.allocate(10)));
      assertEquals(-1, stream.read(-1, ByteBuffer.allocate(10)));
      assertEquals(0, stream.read(10, ByteBuffer.allocate(0)));
      assertThrows(EOFException.class, () -> stream.readFully(plainText.length - 10, new byte[20]));
      assertEquals(777, stream.getPos());

      // The sequential read continues where it left off.
      assertEquals(777, stream.read(sequential, 0, 777));
      assertArrayEquals(Arrays.copyOfRange(plainText, 777, 1554), sequential);
      assertEquals(1554, stream.getPos());
    }
  }

  @Test
  void testCryptoConcurrentPositionedRead() throws Exception {
    final int threads = 8;
    byte[] plainText = randomBytes(2 * CRYPTO_BUFFER_SIZE + 555);
    byte[] key = randomBytes(16);
    byte[] iv = randomBytes(16);

    ExecutorService executor = Executors.newFixedThreadPool(threads);
    try (OzoneCryptoInputStream stream = cryptoInputStream(plainText, key, iv, 0)) {
      CountDownLatch start = new CountDownLatch(1);
      List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        final int position = t * 1234;
        final int length = plainText.length - position;
        futures.add(executor.submit(() -> {
          start.await();
          for (int i = 0; i < 20; i++) {
            ByteBuffer buf = ByteBuffer.allocate(length);
            assertEquals(length, stream.read(position, buf));
            assertArrayEquals(Arrays.copyOfRange(plainText, position, position + length), buf.array());
          }
          return null;
        }));
      }

      start.countDown();
      // Interleave sequential reads on the same stream while the positioned reads are running,
      // alternating both sequential paths: the byte[] read of this class, which adjusts the read to the
      // Crypto buffer boundaries, and the inherited read(ByteBuffer), which is the one
      // MultipartInputStream uses for a part.
      byte[] expected = Arrays.copyOfRange(plainText, 4321, 4421);
      for (int i = 0; i < 20; i++) {
        stream.seek(4321);
        if (i % 2 == 0) {
          byte[] sequential = new byte[100];
          assertEquals(100, stream.read(sequential, 0, 100));
          assertArrayEquals(expected, sequential);
        } else {
          ByteBuffer sequential = ByteBuffer.allocate(100);
          while (sequential.hasRemaining()) {
            assertTrue(stream.read(sequential) > 0);
          }
          assertArrayEquals(expected, sequential.array());
        }
        assertEquals(4421, stream.getPos());
      }

      for (Future<?> future : futures) {
        future.get(120, TimeUnit.SECONDS);
      }
      assertEquals(4421, stream.getPos());
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void testCryptoMultipartPositionedRead() throws IOException {
    byte[] key = randomBytes(16);
    byte[] iv = randomBytes(16);
    // Each part is encrypted independently with the same key and IV, exactly as RpcClient reads them back.
    byte[] part0 = randomBytes(CRYPTO_BUFFER_SIZE + 321);
    byte[] part1 = randomBytes(CRYPTO_BUFFER_SIZE + 123);
    byte[] keyData = concat(part0, part1);

    List<OzoneCryptoInputStream> parts = Arrays.asList(
        cryptoInputStream(part0, key, iv, 0), cryptoInputStream(part1, key, iv, 1));
    try (OzoneInputStream stream = new OzoneInputStream(new MultipartInputStream(KEY, parts))) {
      assertEquals(0, stream.getPos());
      assertFalse(stream.hasCapability(StreamCapabilities.PREADBYTEBUFFER));

      // A read spanning the part boundary.
      int position = part0.length - 500;
      int length = 1000;
      ByteBuffer buf = ByteBuffer.allocate(length);
      assertEquals(length, stream.read(position, buf));
      assertArrayEquals(Arrays.copyOfRange(keyData, position, position + length), buf.array());
      assertEquals(0, stream.getPos());

      // The whole key in one positioned read.
      buf = ByteBuffer.allocate(keyData.length);
      stream.readFully(0, buf);
      assertArrayEquals(keyData, buf.array());
      assertEquals(0, stream.getPos());

      // Sequential reads still see the right data after the positioned reads.
      byte[] sequential = new byte[keyData.length];
      int read = 0;
      while (read < sequential.length) {
        int numBytesRead = stream.read(sequential, read, sequential.length - read);
        assertTrue(numBytesRead > 0);
        read += numBytesRead;
      }
      assertArrayEquals(keyData, sequential);
    }
  }

  @Test
  void testCryptoMultipartConcurrentPositionedRead() throws Exception {
    final int threads = 8;
    byte[] key = randomBytes(16);
    byte[] iv = randomBytes(16);
    byte[] part0 = randomBytes(CRYPTO_BUFFER_SIZE + 321);
    byte[] part1 = randomBytes(CRYPTO_BUFFER_SIZE + 123);
    byte[] keyData = concat(part0, part1);

    List<OzoneCryptoInputStream> parts = Arrays.asList(
        cryptoInputStream(part0, key, iv, 0), cryptoInputStream(part1, key, iv, 1));
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    try (OzoneInputStream stream = new OzoneInputStream(new MultipartInputStream(KEY, parts))) {
      CountDownLatch start = new CountDownLatch(1);
      List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        final int position = t * 1000;
        final int length = keyData.length - position;
        futures.add(executor.submit(() -> {
          start.await();
          for (int i = 0; i < 20; i++) {
            ByteBuffer buf = ByteBuffer.allocate(length);
            assertEquals(length, stream.read(position, buf));
            assertArrayEquals(Arrays.copyOfRange(keyData, position, position + length), buf.array());
          }
          return null;
        }));
      }

      start.countDown();
      // MultipartInputStream.read(long, ByteBuffer) is lock-free, so the positioned reads above run
      // concurrently with these sequential ByteBuffer reads, which reach the very same part streams
      // through MultipartInputStream.readWithStrategy -> part.read(ByteBuffer).
      byte[] expected = Arrays.copyOfRange(keyData, 500, keyData.length);
      for (int i = 0; i < 20; i++) {
        stream.seek(500);
        ByteBuffer sequential = ByteBuffer.allocate(keyData.length - 500);
        while (sequential.hasRemaining()) {
          assertTrue(stream.read(sequential) > 0);
        }
        assertArrayEquals(expected, sequential.array());
        assertEquals(keyData.length, stream.getPos());
      }

      for (Future<?> future : futures) {
        future.get(120, TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdownNow();
    }
  }
}
