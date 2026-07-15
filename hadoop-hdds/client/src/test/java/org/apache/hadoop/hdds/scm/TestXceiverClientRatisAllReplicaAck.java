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

package org.apache.hadoop.hdds.scm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerProto;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.exceptions.NotReplicatedException;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link XceiverClientRatis#watchForCommit(long)} with ozone.client.all.replica.applied.ack on and off,
 * driving the watch through the {@link ErrorInjector} seam so that no RaftClient is needed.
 */
class TestXceiverClientRatisAllReplicaAck {

  private static final String ALL_REPLICA_ACK_KEY = "ozone.client.all.replica.applied.ack";
  private static final String WATCH_TYPE_KEY = "hdds.ratis.client.request.watch.type";
  private static final long SEED_INDEX = 5;
  private static final long WATCH_INDEX = 10;

  /** Records every watch call and answers ALL_COMMITTED and MAJORITY_COMMITTED watches with the configured futures. */
  private static final class WatchInjector implements ErrorInjector {
    private final List<String> watches = Collections.synchronizedList(new ArrayList<>());
    private final CompletableFuture<RaftClientReply> allCommittedReply;
    private final CompletableFuture<RaftClientReply> majorityCommittedReply;

    WatchInjector(CompletableFuture<RaftClientReply> allCommittedReply,
        CompletableFuture<RaftClientReply> majorityCommittedReply) {
      this.allCommittedReply = allCommittedReply;
      this.majorityCommittedReply = majorityCommittedReply;
    }

    @Override
    public RaftClientReply getResponse(ContainerCommandRequestProto request, ClientId id, Pipeline pipeline) {
      return null;
    }

    @Override
    public CompletableFuture<RaftClientReply> watch(long index, ReplicationLevel level, Pipeline pipeline) {
      watches.add(index + ":" + level);
      return level == ReplicationLevel.ALL_COMMITTED ? allCommittedReply : majorityCommittedReply;
    }
  }

  private static OzoneConfiguration newConf(boolean allReplicaAppliedAck, ReplicationLevel watchType) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(ALL_REPLICA_ACK_KEY, allReplicaAppliedAck);
    conf.set(WATCH_TYPE_KEY, watchType.name());
    return conf;
  }

  private static XceiverClientRatis newClient(Pipeline pipeline, boolean allReplicaAppliedAck,
      ErrorInjector injector) {
    XceiverClientRatis client = XceiverClientRatis.newXceiverClientRatis(pipeline,
        newConf(allReplicaAppliedAck, ReplicationLevel.ALL_COMMITTED), null, injector);
    for (DatanodeDetails dn : pipeline.getNodes()) {
      client.getCommitInfoMap().put(UUID.fromString(dn.getUuidString()), SEED_INDEX);
    }
    return client;
  }

  private static CommitInfoProto commitInfo(DatanodeDetails dn, long commitIndex) {
    return CommitInfoProto.newBuilder()
        .setServer(RaftPeerProto.newBuilder().setId(RatisHelper.toRaftPeerId(dn).toByteString()))
        .setCommitIndex(commitIndex)
        .build();
  }

  /** Commit infos where the first pipeline node lags behind {@link #WATCH_INDEX} and the others have reached it. */
  private static List<CommitInfoProto> commitInfosWithLaggingNode(Pipeline pipeline) {
    List<CommitInfoProto> commitInfos = new ArrayList<>();
    List<DatanodeDetails> nodes = pipeline.getNodes();
    commitInfos.add(commitInfo(nodes.get(0), WATCH_INDEX - 1));
    for (int i = 1; i < nodes.size(); i++) {
      commitInfos.add(commitInfo(nodes.get(i), WATCH_INDEX));
    }
    return commitInfos;
  }

  private static CompletableFuture<RaftClientReply> failedWith(Throwable t) {
    CompletableFuture<RaftClientReply> future = new CompletableFuture<>();
    future.completeExceptionally(t);
    return future;
  }

  private static NotReplicatedException notReplicated(Pipeline pipeline) {
    return new NotReplicatedException(1, ReplicationLevel.ALL_COMMITTED, WATCH_INDEX,
        commitInfosWithLaggingNode(pipeline));
  }

  private static RaftClientReply majorityReply(Pipeline pipeline) {
    return RaftClientReply.newBuilder()
        .setClientId(ClientId.randomId())
        .setServerId(RatisHelper.toRaftPeerId(pipeline.getNodes().get(0)))
        .setGroupId(RaftGroupId.valueOf(pipeline.getId().getId()))
        .setCallId(1)
        .setSuccess()
        .setLogIndex(WATCH_INDEX)
        .setCommitInfos(commitInfosWithLaggingNode(pipeline))
        .build();
  }

  private static List<String> uuids(List<DatanodeDetails> datanodes) {
    return datanodes.stream().map(DatanodeDetails::getUuidString).collect(Collectors.toList());
  }

  @Test
  void notReplicatedFailsFastWhenAllReplicaAckEnabled() {
    Pipeline pipeline = MockPipeline.createRatisPipeline();
    WatchInjector injector = new WatchInjector(failedWith(notReplicated(pipeline)), null);
    XceiverClientRatis client = newClient(pipeline, true, injector);

    ExecutionException ex = assertThrows(ExecutionException.class, () -> client.watchForCommit(WATCH_INDEX).get());

    AllReplicaWatchFailedException failure = assertInstanceOf(AllReplicaWatchFailedException.class, ex.getCause());
    assertEquals(WATCH_INDEX, failure.getWatchIndex());
    assertEquals(Collections.singletonList(pipeline.getNodes().get(0).getUuidString()),
        uuids(failure.getFailedDatanodes()));
    assertNotNull(HddsClientUtils.containsException(failure, NotReplicatedException.class));
    assertEquals(Collections.singletonList(WATCH_INDEX + ":" + ReplicationLevel.ALL_COMMITTED), injector.watches);
    assertEquals(3, client.getCommitInfoMap().size());
    assertEquals(SEED_INDEX, client.getReplicatedMinCommitIndex());
  }

  @Test
  void notReplicatedFallsBackToMajorityWhenAllReplicaAckDisabled() throws Exception {
    Pipeline pipeline = MockPipeline.createRatisPipeline();
    WatchInjector injector = new WatchInjector(failedWith(notReplicated(pipeline)), null);
    XceiverClientRatis client = newClient(pipeline, false, injector);

    XceiverClientReply reply = client.watchForCommit(WATCH_INDEX).get();

    assertEquals(WATCH_INDEX, reply.getLogIndex());
    assertEquals(Collections.singletonList(pipeline.getNodes().get(0).getUuidString()), uuids(reply.getDatanodes()));
    assertEquals(Collections.singletonList(WATCH_INDEX + ":" + ReplicationLevel.ALL_COMMITTED), injector.watches);
    assertEquals(2, client.getCommitInfoMap().size());
  }

  @Test
  void genericFailureFailsFastWhenAllReplicaAckEnabled() {
    Pipeline pipeline = MockPipeline.createRatisPipeline();
    IOException injected = new IOException("injected");
    WatchInjector injector = new WatchInjector(failedWith(injected),
        CompletableFuture.completedFuture(majorityReply(pipeline)));
    XceiverClientRatis client = newClient(pipeline, true, injector);

    ExecutionException ex = assertThrows(ExecutionException.class, () -> client.watchForCommit(WATCH_INDEX).get());

    assertSame(injected, ex.getCause());
    assertEquals(Collections.singletonList(WATCH_INDEX + ":" + ReplicationLevel.ALL_COMMITTED), injector.watches);
    assertEquals(3, client.getCommitInfoMap().size());
  }

  @Test
  void genericFailureFallsBackToMajorityWatchWhenAllReplicaAckDisabled() throws Exception {
    Pipeline pipeline = MockPipeline.createRatisPipeline();
    WatchInjector injector = new WatchInjector(failedWith(new IOException("injected")),
        CompletableFuture.completedFuture(majorityReply(pipeline)));
    XceiverClientRatis client = newClient(pipeline, false, injector);

    XceiverClientReply reply = client.watchForCommit(WATCH_INDEX).get();

    assertEquals(WATCH_INDEX, reply.getLogIndex());
    assertEquals(Collections.singletonList(pipeline.getNodes().get(0).getUuidString()), uuids(reply.getDatanodes()));
    List<String> expected = new ArrayList<>();
    expected.add(WATCH_INDEX + ":" + ReplicationLevel.ALL_COMMITTED);
    expected.add(WATCH_INDEX + ":" + ReplicationLevel.MAJORITY_COMMITTED);
    assertEquals(expected, injector.watches);
    assertEquals(2, client.getCommitInfoMap().size());
  }

  @Test
  void constructorRejectsMajorityWatchTypeWhenAllReplicaAckEnabled() {
    Pipeline pipeline = MockPipeline.createRatisPipeline();
    OzoneConfiguration conf = newConf(true, ReplicationLevel.MAJORITY_COMMITTED);

    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> XceiverClientRatis.newXceiverClientRatis(pipeline, conf, null, null));

    assertTrue(ex.getMessage().contains(ALL_REPLICA_ACK_KEY), ex.getMessage());
    assertTrue(ex.getMessage().contains(WATCH_TYPE_KEY), ex.getMessage());
  }

  @Test
  void constructorAcceptsMajorityWatchTypeWhenAllReplicaAckDisabled() {
    Pipeline pipeline = MockPipeline.createRatisPipeline();
    OzoneConfiguration conf = newConf(false, ReplicationLevel.MAJORITY_COMMITTED);

    assertNotNull(XceiverClientRatis.newXceiverClientRatis(pipeline, conf, null, null));
  }
}
