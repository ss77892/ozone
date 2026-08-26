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

import static org.apache.hadoop.ozone.OzoneConsts.MB;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.storage.BlockExtendedInputStream;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.junit.jupiter.api.Test;

/**
 * Test KeyInputStream with EC keys.
 */
public class TestKeyInputStreamEC {

  private static final int EOF = -1;
  /** Two unequal parts, so a read at 900 of 300 bytes crosses the part boundary. */
  private static final int[] PART_LENGTHS = {1000, 700};

  private OzoneConfiguration conf = new OzoneConfiguration();

  @Test
  public void testReadAgainstLargeBlockGroup() throws IOException {
    int dataBlocks = 10;
    int parityBlocks = 4;
    ECReplicationConfig ec10And4RepConfig = new ECReplicationConfig(dataBlocks,
        parityBlocks, ECReplicationConfig.EcCodec.RS, (int)(1 * MB));
    // default blockSize of 256MB with EC 10+4 makes a large block group
    long blockSize = 256 * MB;
    long blockLength = dataBlocks * blockSize;
    OmKeyInfo keyInfo = createOmKeyInfo(ec10And4RepConfig,
        dataBlocks + parityBlocks, blockLength);

    BlockExtendedInputStream blockInputStream =
        new ECStreamTestUtil.TestBlockInputStream(new BlockID(1, 1),
        blockLength, ByteBuffer.allocate(100));

    BlockInputStreamFactory mockStreamFactory =
        mock(BlockInputStreamFactory.class);
    when(mockStreamFactory.create(any(), any(), any(), any(),
        any(), any(), any())).thenReturn(blockInputStream);

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    try (LengthInputStream kis = KeyInputStream.getFromOmKeyInfo(keyInfo,
        null,  null, mockStreamFactory,
        clientConfig)) {
      byte[] buf = new byte[100];
      int readBytes = kis.read(buf, 0, 100);
      assertEquals(100, readBytes);
    }
  }

  @Test
  public void testPreadByteBufferCapabilityIsFalse() throws IOException {
    byte[] keyData = randomKeyData(PART_LENGTHS);
    try (LengthInputStream lis = createECKeyStream(PART_LENGTHS, keyData)) {
      KeyInputStream kis = (KeyInputStream) lis.getWrappedStream();
      // Every EC part uses the serialized default positioned read, which moves the part cursor and puts
      // it back, so the key stream must not claim the PREADBYTEBUFFER capability.
      assertFalse(kis.hasCapability(StreamCapabilities.PREADBYTEBUFFER));
      assertTrue(kis.hasCapability(StreamCapabilities.READBYTEBUFFER));
    }
  }

  @Test
  public void testPositionedReadAcrossPartBoundary() throws IOException {
    byte[] keyData = randomKeyData(PART_LENGTHS);
    try (LengthInputStream lis = createECKeyStream(PART_LENGTHS, keyData)) {
      KeyInputStream kis = (KeyInputStream) lis.getWrappedStream();

      // Move the cursor off zero so restoring it is actually asserted.
      byte[] sequential = new byte[100];
      assertEquals(100, kis.read(sequential, 0, 100));
      assertArrayEquals(Arrays.copyOfRange(keyData, 0, 100), sequential);
      assertEquals(100, kis.getPos());

      // 900 - 1200 crosses the boundary between part 0 (1000 bytes) and part 1.
      int position = 900;
      int length = 300;
      byte[] expected = Arrays.copyOfRange(keyData, position, position + length);

      ByteBuffer readBuffer = ByteBuffer.allocate(length);
      assertEquals(length, kis.read(position, readBuffer));
      assertArrayEquals(expected, readBuffer.array());
      assertEquals(100, kis.getPos());

      byte[] readArray = new byte[length];
      assertEquals(length, kis.read(position, readArray, 0, length));
      assertArrayEquals(expected, readArray);
      assertEquals(100, kis.getPos());

      // A positioned read of the whole key, spanning every part.
      byte[] wholeKey = new byte[keyData.length];
      kis.readFully(0, wholeKey);
      assertArrayEquals(keyData, wholeKey);
      assertEquals(100, kis.getPos());
    }
  }

  @Test
  public void testReadFullyPastEndOfKeyThrowsEOF() throws IOException {
    byte[] keyData = randomKeyData(PART_LENGTHS);
    try (LengthInputStream lis = createECKeyStream(PART_LENGTHS, keyData)) {
      KeyInputStream kis = (KeyInputStream) lis.getWrappedStream();

      // The key is 1700 bytes, so this runs out of data in the last part.
      byte[] readArray = new byte[200];
      assertThrows(EOFException.class, () -> kis.readFully(1600, readArray));
      assertThrows(EOFException.class,
          () -> kis.readFully(1700, ByteBuffer.allocate(1)));
      assertEquals(EOF, kis.read(1700, ByteBuffer.allocate(1)));
      assertEquals(0, kis.getPos());
    }
  }

  private static byte[] randomKeyData(int[] partLengths) {
    int keyLength = 0;
    for (int partLength : partLengths) {
      keyLength += partLength;
    }
    byte[] keyData = new byte[keyLength];
    ThreadLocalRandom.current().nextBytes(keyData);
    return keyData;
  }

  /**
   * Builds an EC KeyInputStream with one TestBlockInputStream per part, each serving its slice of
   * keyData.
   */
  private LengthInputStream createECKeyStream(int[] partLengths,
      byte[] keyData) throws IOException {
    ECReplicationConfig ecRepConfig = new ECReplicationConfig(3, 2,
        ECReplicationConfig.EcCodec.RS, (int) (1 * MB));
    OmKeyInfo keyInfo = createOmKeyInfo(ecRepConfig, 5, partLengths);

    List<BlockExtendedInputStream> partStreams = new ArrayList<>();
    int offset = 0;
    for (int i = 0; i < partLengths.length; i++) {
      ByteBuffer partData = ByteBuffer.wrap(
          Arrays.copyOfRange(keyData, offset, offset + partLengths[i]));
      partStreams.add(new ECStreamTestUtil.TestBlockInputStream(
          new BlockID(1, i + 1), partLengths[i], partData));
      offset += partLengths[i];
    }

    Iterator<BlockExtendedInputStream> streams = partStreams.iterator();
    BlockInputStreamFactory mockStreamFactory =
        mock(BlockInputStreamFactory.class);
    when(mockStreamFactory.create(any(), any(), any(), any(),
        any(), any(), any())).thenAnswer(invocation -> streams.next());

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    return KeyInputStream.getFromOmKeyInfo(keyInfo, null, null,
        mockStreamFactory, clientConfig);
  }

  private OmKeyInfo createOmKeyInfo(ReplicationConfig repConf, int nodeCount,
      int[] blockLengths) {
    Map<DatanodeDetails, Integer> dnMap = new HashMap<>();
    for (int i = 0; i < nodeCount; i++) {
      dnMap.put(MockDatanodeDetails.randomDatanodeDetails(), i + 1);
    }

    Pipeline pipeline = Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.CLOSED)
        .setId(PipelineID.randomId())
        .setNodes(new ArrayList<>(dnMap.keySet()))
        .setReplicaIndexes(dnMap)
        .setReplicationConfig(repConf)
        .build();

    List<OmKeyLocationInfo> locations = new ArrayList<>();
    long keyLength = 0;
    for (int i = 0; i < blockLengths.length; i++) {
      locations.add(new OmKeyLocationInfo.Builder()
          .setBlockID(new BlockID(1, i + 1))
          .setLength(blockLengths[i])
          .setOffset(0)
          .setPipeline(pipeline)
          .setPartNumber(0)
          .build());
      keyLength += blockLengths[i];
    }

    return new OmKeyInfo.Builder()
        .setBucketName("bucket")
        .setVolumeName("volume")
        .setDataSize(keyLength)
        .setKeyName("someKey")
        .setReplicationConfig(repConf)
        .addOmKeyLocationInfoGroup(new OmKeyLocationInfoGroup(0, locations))
        .build();
  }

  private OmKeyInfo createOmKeyInfo(ReplicationConfig repConf,
      int nodeCount, long blockLength) {
    Map<DatanodeDetails, Integer> dnMap = new HashMap<>();
    for (int i = 0; i < nodeCount; i++) {
      dnMap.put(MockDatanodeDetails.randomDatanodeDetails(), i + 1);
    }

    Pipeline pipeline = Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.CLOSED)
        .setId(PipelineID.randomId())
        .setNodes(new ArrayList<>(dnMap.keySet()))
        .setReplicaIndexes(dnMap)
        .setReplicationConfig(repConf)
        .build();

    OmKeyLocationInfo blockInfo = new OmKeyLocationInfo.Builder()
        .setBlockID(new BlockID(1, 1))
        .setLength(blockLength)
        .setOffset(0)
        .setPipeline(pipeline)
        .setPartNumber(0)
        .build();

    List<OmKeyLocationInfo> locations = new ArrayList<>();
    locations.add(blockInfo);
    return new OmKeyInfo.Builder()
        .setBucketName("bucket")
        .setVolumeName("volume")
        .setDataSize(blockLength)
        .setKeyName("someKey")
        .setReplicationConfig(repConf)
        .addOmKeyLocationInfoGroup(new OmKeyLocationInfoGroup(0, locations))
        .build();
  }
}
