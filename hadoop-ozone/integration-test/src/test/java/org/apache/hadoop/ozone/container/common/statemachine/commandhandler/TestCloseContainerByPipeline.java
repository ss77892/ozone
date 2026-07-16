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

package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT;
import static org.apache.hadoop.ozone.container.OzoneTestHelper.validateData;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.tag.Unhealthy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test container closing.
 */
public class TestCloseContainerByPipeline {

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;
  private static OzoneClient client;
  private static ObjectStore objectStore;

  @BeforeAll
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, "1");
    conf.setInt(OZONE_DATANODE_PIPELINE_LIMIT, 2);
    conf.setInt(ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT, 15);

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(10)
        .build();
    cluster.waitForClusterToBeReady();
    client = OzoneClientFactory.getRpcClient(conf);
    objectStore = client.getObjectStore();
    objectStore.createVolume("test");
    objectStore.getVolume("test").createBucket("test");
  }

  @AfterAll
  public static void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testIfCloseContainerCommandHandlerIsInvoked() throws Exception {
    String keyName = "testIfCloseContainerCommandHandlerIsInvoked";
    OzoneOutputStream key = objectStore.getVolume("test").getBucket("test")
        .createKey(keyName, 1024, ReplicationType.RATIS, ReplicationFactor.ONE,
            new HashMap<>());
    key.write(keyName.getBytes(UTF_8));
    key.close();

    //get the name of a valid container
    OmKeyArgs keyArgs =
        new OmKeyArgs.Builder().setVolumeName("test").setBucketName("test")
            .setReplicationConfig(RatisReplicationConfig.getInstance(ONE))
            .setDataSize(1024)
            .setKeyName(keyName).build();
    OmKeyLocationInfo omKeyLocationInfo =
        cluster.getOzoneManager().lookupKey(keyArgs).getKeyLocationVersions()
            .get(0).getBlocksLatestVersionOnly().get(0);

    long containerID = omKeyLocationInfo.getContainerID();
    ContainerInfo container = cluster.getStorageContainerManager()
        .getContainerManager().getContainer(ContainerID.valueOf(containerID));
    Pipeline pipeline = cluster.getStorageContainerManager()
        .getPipelineManager().getPipeline(container.getPipelineID());
    List<DatanodeDetails> datanodes = pipeline.getNodes();
    assertEquals(1, datanodes.size());

    DatanodeDetails datanodeDetails = datanodes.get(0);
    HddsDatanodeService datanodeService = null;
    assertFalse(isContainerClosed(cluster, containerID, datanodeDetails));
    for (HddsDatanodeService datanodeServiceItr : cluster.getHddsDatanodes()) {
      if (datanodeDetails.equals(datanodeServiceItr.getDatanodeDetails())) {
        datanodeService = datanodeServiceItr;
        break;
      }
    }
    CommandHandler closeContainerHandler =
        datanodeService.getDatanodeStateMachine().getCommandDispatcher()
            .getCloseContainerHandler();
    int lastInvocationCount = closeContainerHandler.getInvocationCount();
    //send the order to close the container
    SCMCommand<?> command = new CloseContainerCommand(
        containerID, pipeline.getId());
    command.setTerm(
        cluster.getStorageContainerManager().getScmContext().getTermOfLeader());
    cluster.getStorageContainerManager().getScmNodeManager()
        .addDatanodeCommand(datanodeDetails.getID(), command);
    GenericTestUtils
        .waitFor(() -> isContainerClosed(cluster, containerID, datanodeDetails),
            500, 5 * 1000);
    // Make sure the closeContainerCommandHandler is Invoked
    assertThat(closeContainerHandler.getInvocationCount()).isGreaterThan(lastInvocationCount);
  }

  @Test
  public void testCloseContainerViaStandAlone()
      throws IOException, TimeoutException, InterruptedException {

    OzoneOutputStream key = objectStore.getVolume("test").getBucket("test")
        .createKey("standalone", 1024, ReplicationType.RATIS,
            ReplicationFactor.ONE, new HashMap<>());
    key.write("standalone".getBytes(UTF_8));
    key.close();

    //get the name of a valid container
    OmKeyArgs keyArgs =
        new OmKeyArgs.Builder().setVolumeName("test").setBucketName("test")
            .setReplicationConfig(RatisReplicationConfig.getInstance(ONE))
            .setDataSize(1024)
            .setKeyName("standalone")
            .build();

    OmKeyLocationInfo omKeyLocationInfo =
        cluster.getOzoneManager().lookupKey(keyArgs).getKeyLocationVersions()
            .get(0).getBlocksLatestVersionOnly().get(0);

    long containerID = omKeyLocationInfo.getContainerID();
    ContainerInfo container = cluster.getStorageContainerManager()
        .getContainerManager().getContainer(ContainerID.valueOf(containerID));
    Pipeline pipeline = cluster.getStorageContainerManager()
        .getPipelineManager().getPipeline(container.getPipelineID());
    List<DatanodeDetails> datanodes = pipeline.getNodes();
    assertEquals(1, datanodes.size());

    DatanodeDetails datanodeDetails = datanodes.get(0);
    assertFalse(isContainerClosed(cluster, containerID, datanodeDetails));

    // Send the order to close the container, give random pipeline id so that
    // the container will not be closed via RATIS
    SCMCommand<?> command = new CloseContainerCommand(
        containerID, pipeline.getId());
    command.setTerm(
        cluster.getStorageContainerManager().getScmContext().getTermOfLeader());
    cluster.getStorageContainerManager().getScmNodeManager()
        .addDatanodeCommand(datanodeDetails.getID(), command);

    //double check if it's really closed (waitFor also throws an exception)
    // TODO: change the below line after implementing QUASI_CLOSED to CLOSED
    // logic. The container will be QUASI closed as of now
    GenericTestUtils
        .waitFor(() -> isContainerClosed(cluster, containerID, datanodeDetails),
            500, 5 * 1000);
    assertTrue(isContainerClosed(cluster, containerID, datanodeDetails));

    cluster.getStorageContainerManager().getPipelineManager()
        .closePipeline(pipeline.getId());
    Thread.sleep(5000);
    // Pipeline close should not affect a container in CLOSED state
    assertTrue(isContainerClosed(cluster, containerID, datanodeDetails));
  }

  @Test
  public void testCloseContainerViaRatis() throws IOException,
      TimeoutException, InterruptedException {

    OzoneOutputStream key = objectStore.getVolume("test").getBucket("test")
        .createKey("ratis", 1024, ReplicationType.RATIS,
            ReplicationFactor.THREE, new HashMap<>());
    key.write("ratis".getBytes(UTF_8));
    key.close();

    //get the name of a valid container
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName("test").
        setBucketName("test")
        .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
        .setDataSize(1024)
        .setKeyName("ratis").build();

    OmKeyLocationInfo omKeyLocationInfo =
        cluster.getOzoneManager().lookupKey(keyArgs).getKeyLocationVersions()
            .get(0).getBlocksLatestVersionOnly().get(0);

    long containerID = omKeyLocationInfo.getContainerID();
    ContainerInfo container = cluster.getStorageContainerManager()
        .getContainerManager().getContainer(ContainerID.valueOf(containerID));
    Pipeline pipeline = cluster.getStorageContainerManager()
        .getPipelineManager().getPipeline(container.getPipelineID());
    List<DatanodeDetails> datanodes = pipeline.getNodes();
    assertEquals(3, datanodes.size());

    List<DBHandle> metadataStores = new ArrayList<>(datanodes.size());
    for (DatanodeDetails details : datanodes) {
      assertFalse(isContainerClosed(cluster, containerID, details));
      //send the order to close the container
      SCMCommand<?> command = new CloseContainerCommand(
          containerID, pipeline.getId());
      command.setTerm(cluster.getStorageContainerManager()
          .getScmContext().getTermOfLeader());
      cluster.getStorageContainerManager().getScmNodeManager()
          .addDatanodeCommand(details.getID(), command);
      int index = cluster.getHddsDatanodeIndex(details);
      Container dnContainer = cluster.getHddsDatanodes().get(index)
          .getDatanodeStateMachine().getContainer().getContainerSet()
          .getContainer(containerID);
      try (DBHandle store = BlockUtils.getDB(
          (KeyValueContainerData) dnContainer.getContainerData(), conf)) {
        metadataStores.add(store);
      }
    }

    // There should be as many rocks db as the number of datanodes in pipeline.
    assertEquals(datanodes.size(), metadataStores.stream().distinct().count());

    // Make sure that it is CLOSED
    for (DatanodeDetails datanodeDetails : datanodes) {
      GenericTestUtils.waitFor(
          () -> isContainerClosed(cluster, containerID, datanodeDetails), 500,
          15 * 1000);
      //double check if it's really closed (waitFor also throws an exception)
      assertTrue(isContainerClosed(cluster, containerID, datanodeDetails));
    }
  }

  @Unhealthy("Failing with timeout")
  @Test
  public void testQuasiCloseTransitionViaRatis()
      throws IOException, TimeoutException, InterruptedException {

    String keyName = "testQuasiCloseTransitionViaRatis";
    OzoneOutputStream key = objectStore.getVolume("test").getBucket("test")
        .createKey(keyName, 1024, ReplicationType.RATIS,
            ReplicationFactor.ONE, new HashMap<>());
    key.write(keyName.getBytes(UTF_8));
    key.close();

    OmKeyArgs keyArgs =
        new OmKeyArgs.Builder().setVolumeName("test").setBucketName("test")
            .setReplicationConfig(RatisReplicationConfig.getInstance(ONE))
            .setDataSize(1024)
            .setKeyName(keyName)
            .build();

    OmKeyLocationInfo omKeyLocationInfo =
        cluster.getOzoneManager().lookupKey(keyArgs).getKeyLocationVersions()
            .get(0).getBlocksLatestVersionOnly().get(0);

    long containerID = omKeyLocationInfo.getContainerID();
    ContainerInfo container = cluster.getStorageContainerManager()
        .getContainerManager().getContainer(ContainerID.valueOf(containerID));
    Pipeline pipeline = cluster.getStorageContainerManager()
        .getPipelineManager().getPipeline(container.getPipelineID());
    List<DatanodeDetails> datanodes = pipeline.getNodes();
    assertEquals(1, datanodes.size());

    DatanodeDetails datanodeDetails = datanodes.get(0);
    assertFalse(isContainerClosed(cluster, containerID, datanodeDetails));

    // close the pipeline
    cluster.getStorageContainerManager()
        .getPipelineManager().closePipeline(pipeline.getId());

    // All the containers in OPEN or CLOSING state should transition to
    // QUASI-CLOSED after pipeline close
    GenericTestUtils.waitFor(
        () -> isContainerQuasiClosed(cluster, containerID, datanodeDetails),
        500, 5 * 1000);
    assertTrue(
        isContainerQuasiClosed(cluster, containerID, datanodeDetails));

    // Send close container command from SCM to datanode with forced flag as
    // true
    SCMCommand<?> command = new CloseContainerCommand(
        containerID, pipeline.getId(), true);
    command.setTerm(
        cluster.getStorageContainerManager().getScmContext().getTermOfLeader());
    cluster.getStorageContainerManager().getScmNodeManager()
        .addDatanodeCommand(datanodeDetails.getID(), command);
    GenericTestUtils
        .waitFor(() -> isContainerClosed(
            cluster, containerID, datanodeDetails), 500, 5 * 1000);
    assertTrue(isContainerClosed(cluster, containerID, datanodeDetails));
  }

  /**
   * With hdds.datanode.all.replica.applied.ack on every datanode and
   * ozone.client.all.replica.applied.ack on a dedicated client, a closed key
   * has its block record on all three replicas as soon as close() returns, and
   * closing the pipeline takes the container to CLOSED without passing through
   * QUASI_CLOSED. The shared static client stays in default mode; it was built
   * from the original conf and restarting datanodes does not change it.
   */
  @Test
  public void testAllReplicaAppliedAckClosesDirectly() throws Exception {
    String keyName = "testAllReplicaAppliedAckClosesDirectly";
    byte[] data = keyName.getBytes(UTF_8);
    OzoneConfiguration modeOnConf = new OzoneConfiguration(conf);
    modeOnConf.setBoolean("ozone.client.all.replica.applied.ack", true);
    modeOnConf.setBoolean("ozone.client.stream.putblock.piggybacking", false);
    modeOnConf.setBoolean("ozone.client.incremental.chunk.list", false);

    setDatanodeAllReplicaAppliedAck(true);
    try (OzoneClient modeOnClient = OzoneClientFactory.getRpcClient(modeOnConf)) {
      OzoneOutputStream key = modeOnClient.getObjectStore().getVolume("test").getBucket("test")
          .createKey(keyName, 1024, ReplicationType.RATIS, ReplicationFactor.THREE, new HashMap<>());
      key.write(data);
      key.close();

      OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName("test").setBucketName("test")
          .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
          .setDataSize(1024)
          .setKeyName(keyName).build();
      OmKeyLocationInfo omKeyLocationInfo =
          cluster.getOzoneManager().lookupKey(keyArgs).getKeyLocationVersions()
              .get(0).getBlocksLatestVersionOnly().get(0);

      long containerID = omKeyLocationInfo.getContainerID();
      long localID = omKeyLocationInfo.getLocalID();
      long bcsId = omKeyLocationInfo.getBlockCommitSequenceId();
      ContainerInfo container = cluster.getStorageContainerManager()
          .getContainerManager().getContainer(ContainerID.valueOf(containerID));
      Pipeline pipeline = cluster.getStorageContainerManager()
          .getPipelineManager().getPipeline(container.getPipelineID());
      List<DatanodeDetails> datanodes = pipeline.getNodes();
      assertEquals(3, datanodes.size());

      // No wait: the acknowledged PutBlock was applied on every replica before close() returned.
      for (DatanodeDetails datanodeDetails : datanodes) {
        KeyValueContainerData containerData = getContainerData(containerID, datanodeDetails);
        assertFalse(containerData.isClosed());
        assertFalse(containerData.isQuasiClosed());
        try (DBHandle db = BlockUtils.getDB(containerData, conf)) {
          BlockData blockData = db.getStore().getBlockDataTable().get(containerData.getBlockKey(localID));
          assertNotNull(blockData, "block " + localID + " missing on " + datanodeDetails);
          assertEquals(bcsId, blockData.getBlockCommitSequenceId(), "block BCSID on " + datanodeDetails);
        }
      }

      cluster.getStorageContainerManager().getPipelineManager().closePipeline(pipeline.getId());

      // Every replica goes straight to CLOSED; none is ever observed QUASI_CLOSED.
      for (DatanodeDetails datanodeDetails : datanodes) {
        GenericTestUtils.waitFor(() -> {
          assertFalse(isContainerQuasiClosed(cluster, containerID, datanodeDetails),
              "container " + containerID + " quasi-closed on " + datanodeDetails);
          return isContainerClosed(cluster, containerID, datanodeDetails);
        }, 500, 15 * 1000);
        assertTrue(isContainerClosed(cluster, containerID, datanodeDetails));
        assertFalse(isContainerQuasiClosed(cluster, containerID, datanodeDetails));
      }

      validateData(keyName, data, modeOnClient.getObjectStore(), "test", "test");
    } finally {
      setDatanodeAllReplicaAppliedAck(false);
    }
  }

  /**
   * Toggles hdds.datanode.all.replica.applied.ack on every datanode through a
   * rolling restart: the restarted datanode is rebuilt from its own conf, so
   * the key takes effect, and the shared static client is not affected.
   */
  private static void setDatanodeAllReplicaAppliedAck(boolean enabled) throws Exception {
    for (int i = 0; i < cluster.getHddsDatanodes().size(); i++) {
      cluster.getHddsDatanodes().get(i).getConf()
          .setBoolean(DatanodeConfiguration.ALL_REPLICA_APPLIED_ACK, enabled);
      cluster.restartHddsDatanode(i, false);
    }
    cluster.waitForClusterToBeReady();
    cluster.waitForPipelineTobeReady(THREE, 60 * 1000);
  }

  private KeyValueContainerData getContainerData(long containerID, DatanodeDetails datanode) throws IOException {
    int index = cluster.getHddsDatanodeIndex(datanode);
    return (KeyValueContainerData) cluster.getHddsDatanodes().get(index)
        .getDatanodeStateMachine().getContainer().getContainerSet()
        .getContainer(containerID).getContainerData();
  }

  private Boolean isContainerClosed(MiniOzoneCluster ozoneCluster,
      long containerID,
      DatanodeDetails datanode) {
    ContainerData containerData;
    for (HddsDatanodeService datanodeService : ozoneCluster
        .getHddsDatanodes()) {
      if (datanode.equals(datanodeService.getDatanodeDetails())) {
        containerData =
            datanodeService.getDatanodeStateMachine().getContainer()
                .getContainerSet().getContainer(containerID).getContainerData();
        return containerData.isClosed();
      }
    }
    return false;
  }

  private Boolean isContainerQuasiClosed(MiniOzoneCluster miniCluster,
      long containerID, DatanodeDetails datanode) {
    ContainerData containerData;
    for (HddsDatanodeService datanodeService : miniCluster.getHddsDatanodes()) {
      if (datanode.equals(datanodeService.getDatanodeDetails())) {
        containerData =
            datanodeService.getDatanodeStateMachine().getContainer()
                .getContainerSet().getContainer(containerID).getContainerData();
        return containerData.isQuasiClosed();
      }
    }
    return false;
  }
}
