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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.fs.ByteBufferPositionedReadable;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.CanUnbuffer;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.util.StringUtils;

/**
 * Abstact class which extends InputStream and some common interfaces used by
 * various Ozone InputStream classes.
 */
public abstract class ExtendedInputStream extends InputStream
    implements Seekable, CanUnbuffer, ByteBufferReadable, PositionedReadable, ByteBufferPositionedReadable,
    StreamCapabilities {

  protected static final int EOF = -1;

  /**
   * Positioned read, implemented by serializing against the other synchronized methods of this stream: the
   * cursor is moved to {@code position}, the data is read and the cursor is restored before the lock is
   * released. Subclasses which can read at a position without moving the cursor should override this.
   *
   * @param position the position to read from.
   * @param dst the buffer to read into.
   * @return the number of bytes copied into {@code dst}, or -1 if no byte could be read.
   */
  @Override
  public synchronized int read(long position, ByteBuffer dst) throws IOException {
    if (!dst.hasRemaining()) {
      return 0;
    }
    if (position < 0) {
      // Not every stream throws EOFException from seek() for a negative position, so check it explicitly.
      return EOF;
    }
    final long oldPos = getPos();
    Throwable failure = null;
    try {
      try {
        seek(position);
      } catch (EOFException e) {
        // position is past the end of the stream
        return EOF;
      }
      int totalReadLen = 0;
      while (dst.hasRemaining()) {
        final int readLen = read(dst);
        if (readLen == EOF) {
          break;
        }
        totalReadLen += readLen;
      }
      return totalReadLen == 0 ? EOF : totalReadLen;
    } catch (Throwable t) {
      failure = t;
      throw t;
    } finally {
      try {
        seek(oldPos);
      } catch (IOException e) {
        if (failure == null) {
          throw e;
        }
        failure.addSuppressed(e);
      }
    }
  }

  @Override
  public void readFully(long position, ByteBuffer dst) throws IOException {
    int done = 0;
    while (dst.hasRemaining()) {
      final int readLen = read(position + done, dst);
      if (readLen == EOF) {
        throw new EOFException(FSExceptionMessages.EOF_IN_READ_FULLY);
      }
      done += readLen;
    }
  }

  @Override
  public int read(long position, byte[] b, int off, int len) throws IOException {
    return read(position, ByteBuffer.wrap(b, off, len));
  }

  @Override
  public void readFully(long position, byte[] b, int off, int len) throws IOException {
    readFully(position, ByteBuffer.wrap(b, off, len));
  }

  @Override
  public void readFully(long position, byte[] b) throws IOException {
    readFully(position, b, 0, b.length);
  }

  @Override
  public synchronized int read() throws IOException {
    byte[] buf = new byte[1];
    if (read(buf, 0, 1) == EOF) {
      return EOF;
    }
    return Byte.toUnsignedInt(buf[0]);
  }

  @Override
  public synchronized int read(byte[] b, int off, int len) throws IOException {
    return read(new ByteArrayReader(b, off, len));
  }

  @Override
  public synchronized int read(ByteBuffer byteBuffer) throws IOException {
    return read(new ByteBufferReader(byteBuffer));
  }

  public synchronized int read(ByteReaderStrategy strategy) throws IOException {
    if (strategy.getTargetLength() == 0) {
      return 0;
    }
    return readWithStrategy(strategy);
  }

  /**
   * This must be overridden by the extending classes to call read on the
   * underlying stream they are reading from. The last stream in the chain (the
   * one which provides the actual data) needs to provide a real read via the
   * read methods. For example if a test is extending this class, then it will
   * need to override both read methods above and provide a dummy
   * readWithStrategy implementation, as it will never be called by the tests.
   *
   * @param strategy
   * @throws IOException
   */
  protected abstract int readWithStrategy(ByteReaderStrategy strategy) throws
      IOException;

  @Override
  public synchronized void seek(long l) throws IOException {
    throw new NotImplementedException("Seek is not implemented");
  }

  @Override
  public synchronized boolean seekToNewSource(long l) throws IOException {
    return false;
  }

  @Override
  public boolean hasCapability(String capability) {
    switch (StringUtils.toLowerCase(capability)) {
    case StreamCapabilities.READBYTEBUFFER:
    case StreamCapabilities.UNBUFFER:
      return true;
    case StreamCapabilities.PREADBYTEBUFFER:
      // The default positioned read moves the cursor, subclasses which can do better override this.
      return false;
    default:
      return false;
    }
  }
}
