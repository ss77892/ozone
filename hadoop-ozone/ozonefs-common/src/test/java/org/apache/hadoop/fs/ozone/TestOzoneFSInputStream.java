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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntFunction;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoCodec;
import org.apache.hadoop.crypto.CryptoInputStream;
import org.apache.hadoop.crypto.CryptoOutputStream;
import org.apache.hadoop.crypto.Decryptor;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.ozone.client.io.KeyInputStream;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link OzoneFSInputStream}.
 */
public class TestOzoneFSInputStream {

  private static final List<IntFunction<ByteBuffer>> BUFFER_CONSTRUCTORS =
      ImmutableList.of(ByteBuffer::allocate, ByteBuffer::allocateDirect);

  @Test
  public void readToByteBuffer() throws IOException {
    for (IntFunction<ByteBuffer> constructor : BUFFER_CONSTRUCTORS) {
      for (int streamLength = 1; streamLength <= 10; streamLength++) {
        for (int bufferCapacity = 0; bufferCapacity <= 10; bufferCapacity++) {
          testReadToByteBuffer(constructor, streamLength, bufferCapacity, 0);
          if (bufferCapacity > 1) {
            testReadToByteBuffer(constructor, streamLength, bufferCapacity, 1);
            if (bufferCapacity > 2) {
              testReadToByteBuffer(constructor, streamLength, bufferCapacity,
                  bufferCapacity - 1);
            }
          }
          testReadToByteBuffer(constructor, streamLength, bufferCapacity,
              bufferCapacity);
        }
      }
    }
  }

  private static void testReadToByteBuffer(
      IntFunction<ByteBuffer> bufferConstructor,
      int streamLength, int bufferCapacity,
      int bufferPosition) throws IOException {
    final byte[] source = RandomUtils.secure().randomBytes(streamLength);
    final InputStream input = new ByteArrayInputStream(source);
    final OzoneFSInputStream subject = createTestSubject(input);

    final int expectedReadLength = Math.min(bufferCapacity - bufferPosition,
        input.available());
    final byte[] expectedContent = Arrays.copyOfRange(source, 0,
        expectedReadLength);

    final ByteBuffer buf = bufferConstructor.apply(bufferCapacity);
    buf.position(bufferPosition);

    final int bytesRead = subject.read(buf);

    assertEquals(expectedReadLength, bytesRead);

    final byte[] content = new byte[bytesRead];
    buf.position(bufferPosition);
    buf.get(content);
    assertArrayEquals(expectedContent, content);
  }

  @Test
  public void readEmptyStreamToByteBuffer() throws IOException {
    for (IntFunction<ByteBuffer> constructor : BUFFER_CONSTRUCTORS) {
      final OzoneFSInputStream subject = createTestSubject(emptyStream());
      final ByteBuffer buf = constructor.apply(1);

      final int bytesRead = subject.read(buf);

      assertEquals(-1, bytesRead);
      assertEquals(0, buf.position());
    }
  }

  @Test
  public void bufferPositionUnchangedOnEOF() throws IOException {
    for (IntFunction<ByteBuffer> constructor : BUFFER_CONSTRUCTORS) {
      final OzoneFSInputStream subject = createTestSubject(eofStream());
      final ByteBuffer buf = constructor.apply(123);

      final int bytesRead = subject.read(buf);

      assertEquals(-1, bytesRead);
      assertEquals(0, buf.position());
    }
  }

  @Test
  public void testStreamCapability() throws IOException {
    final OzoneFSInputStream subject = createTestSubject(emptyStream());
    CapableOzoneFSInputStream capableOzoneFSInputStream = null;
    try {
      capableOzoneFSInputStream = new CapableOzoneFSInputStream(subject,
          new FileSystem.Statistics("test"));

      assertTrue(capableOzoneFSInputStream.
          hasCapability(StreamCapabilities.READBYTEBUFFER));
    } finally {
      if (capableOzoneFSInputStream != null) {
        capableOzoneFSInputStream.close();
      }
    }
  }

  @Test
  public void testCryptoStreamUnbuffer()
      throws IOException, GeneralSecurityException {
    KeyInputStream keyInputStream = mock(KeyInputStream.class);
    when(keyInputStream.hasCapability(anyString())).thenReturn(true);

    CryptoCodec codec = mock(CryptoCodec.class);
    when(codec.getCipherSuite()).thenReturn(CipherSuite.AES_CTR_NOPADDING);
    when(codec.getConf()).thenReturn(new Configuration());
    Decryptor decryptor = mock(Decryptor.class);
    when(codec.createDecryptor()).thenReturn(decryptor);
    CryptoInputStream cis = new CryptoInputStream(keyInputStream, codec,
        new byte[0], new byte[0]);
    try {
      cis.unbuffer();
      verify(keyInputStream, times(1)).unbuffer();
    } finally {
      cis.close();
    }
  }

  @Test
  public void positionedReadDelegatesToKeyInputStream() throws IOException {
    final byte[] source = RandomUtils.secure().randomBytes(64);
    final KeyInputStream keyInputStream = mockKeyInputStream(source);
    final FileSystem.Statistics statistics = new FileSystem.Statistics("test");

    try (OzoneFSInputStream subject =
        new OzoneFSInputStream(keyInputStream, statistics)) {
      final ByteBuffer buf = ByteBuffer.allocate(10);
      assertEquals(10, subject.read(5, buf));
      assertArrayEquals(Arrays.copyOfRange(source, 5, 15), buf.array());
      assertEquals(10, statistics.getBytesRead());

      final byte[] bytes = new byte[10];
      assertEquals(10, subject.read(20, bytes, 0, 10));
      assertArrayEquals(Arrays.copyOfRange(source, 20, 30), bytes);
      assertEquals(20, statistics.getBytesRead());

      // Empty buffer, negative position and past-EOF position keep their semantics
      assertEquals(0, subject.read(5, ByteBuffer.allocate(0)));
      assertEquals(-1, subject.read(-1, ByteBuffer.allocate(10)));
      assertEquals(-1, subject.read(source.length, ByteBuffer.allocate(10)));
      assertEquals(20, statistics.getBytesRead());
    }

    verify(keyInputStream, never()).seek(anyLong());
  }

  @Test
  public void positionedReadFullyDelegatesToKeyInputStream() throws IOException {
    final byte[] source = RandomUtils.secure().randomBytes(64);
    final KeyInputStream keyInputStream = mockKeyInputStream(source);
    final FileSystem.Statistics statistics = new FileSystem.Statistics("test");

    try (OzoneFSInputStream subject =
        new OzoneFSInputStream(keyInputStream, statistics)) {
      final ByteBuffer buf = ByteBuffer.allocate(10);
      subject.readFully(5, buf);
      assertFalse(buf.hasRemaining());
      assertArrayEquals(Arrays.copyOfRange(source, 5, 15), buf.array());
      assertEquals(10, statistics.getBytesRead());

      final byte[] bytes = new byte[10];
      subject.readFully(20, bytes, 0, 10);
      assertArrayEquals(Arrays.copyOfRange(source, 20, 30), bytes);
      assertEquals(20, statistics.getBytesRead());

      final byte[] all = new byte[8];
      subject.readFully(30, all);
      assertArrayEquals(Arrays.copyOfRange(source, 30, 38), all);
      assertEquals(28, statistics.getBytesRead());

      // A range which cannot be filled fails, as required by the contract
      assertThrows(EOFException.class,
          () -> subject.readFully(source.length - 1, ByteBuffer.allocate(10)));
      assertThrows(EOFException.class,
          () -> subject.readFully(source.length - 1, new byte[10], 0, 10));
    }

    verify(keyInputStream, never()).seek(anyLong());
  }

  @Test
  public void positionedReadThroughCryptoStream()
      throws IOException, GeneralSecurityException {
    final byte[] plainText = RandomUtils.secure().randomBytes(1024);
    final byte[] key = RandomUtils.secure().randomBytes(16);
    final byte[] iv = RandomUtils.secure().randomBytes(16);
    final CryptoCodec codec = CryptoCodec.getInstance(new Configuration());
    final byte[] cipherText = encrypt(plainText, codec, key, iv);

    final KeyInputStream keyInputStream = mockKeyInputStream(cipherText);
    final FileSystem.Statistics statistics = new FileSystem.Statistics("test");

    try (CryptoInputStream cis =
             new CryptoInputStream(keyInputStream, codec, key, iv);
         OzoneFSInputStream subject = new OzoneFSInputStream(cis, statistics)) {
      final ByteBuffer buf = ByteBuffer.allocate(100);
      assertEquals(100, subject.read(200, buf));
      assertArrayEquals(Arrays.copyOfRange(plainText, 200, 300), buf.array());
      assertEquals(100, statistics.getBytesRead());

      final byte[] bytes = new byte[100];
      assertEquals(100, subject.read(300, bytes, 0, 100));
      assertArrayEquals(Arrays.copyOfRange(plainText, 300, 400), bytes);
      assertEquals(200, statistics.getBytesRead());

      final ByteBuffer fully = ByteBuffer.allocate(100);
      subject.readFully(400, fully);
      assertFalse(fully.hasRemaining());
      assertArrayEquals(Arrays.copyOfRange(plainText, 400, 500), fully.array());

      final byte[] fullyBytes = new byte[100];
      subject.readFully(500, fullyBytes);
      assertArrayEquals(Arrays.copyOfRange(plainText, 500, 600), fullyBytes);
    }

    verify(keyInputStream, never()).seek(anyLong());
  }

  @Test
  public void testStreamCapabilityPreadByteBuffer() throws IOException {
    final KeyInputStream preadCapable = mockKeyInputStream(new byte[0]);
    when(preadCapable.hasCapability(anyString())).thenReturn(true);
    assertCapabilities(preadCapable, true);

    final KeyInputStream notPreadCapable = mockKeyInputStream(new byte[0]);
    when(notPreadCapable.hasCapability(anyString())).thenReturn(false);
    assertCapabilities(notPreadCapable, false);

    // A stream which does not expose its capabilities cannot do a positioned read without moving the cursor
    assertCapabilities(emptyStream(), false);
  }

  private static void assertCapabilities(InputStream wrapped,
      boolean expectedPread) throws IOException {
    try (CapableOzoneFSInputStream subject = new CapableOzoneFSInputStream(
        wrapped, new FileSystem.Statistics("test"))) {
      assertTrue(subject.hasCapability(StreamCapabilities.READBYTEBUFFER));
      assertTrue(subject.hasCapability(StreamCapabilities.UNBUFFER));
      assertEquals(expectedPread,
          subject.hasCapability(StreamCapabilities.PREADBYTEBUFFER));
    }
  }

  private static byte[] encrypt(byte[] plainText, CryptoCodec codec,
      byte[] key, byte[] iv) throws IOException, GeneralSecurityException {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (CryptoOutputStream cos = new CryptoOutputStream(out, codec, key, iv)) {
      cos.write(plainText);
    }
    return out.toByteArray();
  }

  /**
   * A KeyInputStream which serves {@code source} from any position without ever moving its cursor.
   */
  private static KeyInputStream mockKeyInputStream(byte[] source)
      throws IOException {
    final KeyInputStream stream = mock(KeyInputStream.class);
    when(stream.read(anyLong(), any(ByteBuffer.class))).thenAnswer(
        invocation -> copyFrom(source, invocation.getArgument(0),
            invocation.getArgument(1)));
    when(stream.read(anyLong(), any(byte[].class), anyInt(), anyInt()))
        .thenAnswer(invocation -> copyFrom(source, invocation.getArgument(0),
            ByteBuffer.wrap(invocation.getArgument(1),
                invocation.getArgument(2), invocation.getArgument(3))));
    doAnswer(invocation -> fillFrom(source, invocation.getArgument(0),
        invocation.getArgument(1)))
        .when(stream).readFully(anyLong(), any(ByteBuffer.class));
    doAnswer(invocation -> fillFrom(source, invocation.getArgument(0),
        ByteBuffer.wrap(invocation.getArgument(1), invocation.getArgument(2),
            invocation.getArgument(3))))
        .when(stream).readFully(anyLong(), any(byte[].class), anyInt(),
            anyInt());
    doAnswer(invocation -> fillFrom(source, invocation.getArgument(0),
        ByteBuffer.wrap(invocation.getArgument(1))))
        .when(stream).readFully(anyLong(), any(byte[].class));
    return stream;
  }

  private static int copyFrom(byte[] source, long position, ByteBuffer dst) {
    if (!dst.hasRemaining()) {
      return 0;
    }
    if (position < 0 || position >= source.length) {
      return -1;
    }
    final int len = Math.min(dst.remaining(), source.length - (int) position);
    dst.put(source, (int) position, len);
    return len;
  }

  private static Void fillFrom(byte[] source, long position, ByteBuffer dst)
      throws EOFException {
    if (position < 0 || position + dst.remaining() > source.length) {
      throw new EOFException("End of file reached before reading fully.");
    }
    copyFrom(source, position, dst);
    return null;
  }

  private static OzoneFSInputStream createTestSubject(InputStream input) {
    return new OzoneFSInputStream(input,
        new FileSystem.Statistics("test"));
  }

  private static InputStream emptyStream() {
    return new ByteArrayInputStream(new byte[0]);
  }

  private static InputStream eofStream() {
    return new InputStream() {
      @Override
      public int available() {
        return 123;
      }

      @Override
      public int read() {
        return -1;
      }
    };
  }

}
