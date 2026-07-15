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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;

/**
 * Thrown by {@link XceiverClientRatis#watchForCommit(long)} when ozone.client.all.replica.applied.ack is enabled
 * and the ALL_COMMITTED watch fails: there is no fallback to a majority commit, so the write cannot be acknowledged.
 * The original failure (typically a Ratis NotReplicatedException) is kept as the cause so that
 * {@link org.apache.hadoop.hdds.scm.client.HddsClientUtils#checkForException} still finds it.
 */
public class AllReplicaWatchFailedException extends IOException {
  private static final long serialVersionUID = 1L;

  private final long watchIndex;
  private final transient List<DatanodeDetails> failedDatanodes;

  public AllReplicaWatchFailedException(long watchIndex, Collection<DatanodeDetails> failedDatanodes,
      Throwable cause) {
    super("Index " + watchIndex + " was not applied on all replicas; lagging datanodes: " + failedDatanodes, cause);
    this.watchIndex = watchIndex;
    this.failedDatanodes = Collections.unmodifiableList(new ArrayList<>(failedDatanodes));
  }

  public long getWatchIndex() {
    return watchIndex;
  }

  public List<DatanodeDetails> getFailedDatanodes() {
    return failedDatanodes;
  }
}
