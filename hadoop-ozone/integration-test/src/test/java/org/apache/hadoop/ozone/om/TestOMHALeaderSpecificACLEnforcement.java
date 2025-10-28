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

package org.apache.hadoop.ozone.om;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.PERMISSION_DENIED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneNativeAuthorizer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Integration test for OM HA leader-specific ACL enforcement.
 * Demonstrates that ACL check responsibility depends entirely on the current leader,
 * with no expectation that all leaders are synchronized. Each leader enforces
 * ACLs based on its own configuration independently.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestOMHALeaderSpecificACLEnforcement {

  private static final String OM_SERVICE_ID = "om-service-test-admin";
  private static final int NUM_OF_OMS = 3;
  private static final String TEST_USER = "testuser-" + 
      RandomStringUtils.secure().nextAlphabetic(5).toLowerCase(Locale.ROOT);
  private static final String TEST_VOLUME = "testvol-" + 
      RandomStringUtils.secure().nextAlphabetic(5).toLowerCase(Locale.ROOT);
  private static final String ADMIN_VOLUME = "adminvol-" + 
      RandomStringUtils.secure().nextAlphabetic(5).toLowerCase(Locale.ROOT);
  private static final String TEST_BUCKET = "testbucket-" + 
      RandomStringUtils.secure().nextAlphabetic(5).toLowerCase(Locale.ROOT);

  private MiniOzoneHAClusterImpl cluster;
  private OzoneClient client;
  private UserGroupInformation testUserUgi;
  private UserGroupInformation adminUserUgi;
  private OzoneManager theLeaderOM;

  @BeforeAll
  public void init() throws Exception {
    // Create test user
    testUserUgi = UserGroupInformation.createUserForTesting(TEST_USER, new String[]{"testgroup"});
    adminUserUgi = UserGroupInformation.getCurrentUser();
    
    // Set up and start the cluster
    setupCluster();
    
    // Create admin volume that will be used for bucket permission testing
    theLeaderOM = cluster.getOMLeader();
    createAdminVolume();
  }

  @AfterAll
  public void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @BeforeEach
  public void restoreLeadership() throws IOException, InterruptedException, TimeoutException {
    OzoneManager currentLeader = cluster.getOMLeader();
    if (!currentLeader.getOMNodeId().equals(theLeaderOM.getOMNodeId())) {
      currentLeader.transferLeadership(theLeaderOM.getOMNodeId());
      BooleanSupplier leadershipCheck = () -> {
        try {
          return !cluster.getOMLeader().getOMNodeId().equals(currentLeader.getOMNodeId());
        } catch (Exception e) {
          return false;
        }
      };
      GenericTestUtils.waitFor(leadershipCheck, 1000, 30000);
    }
  }

  /**
   * Main test method that validates leader-specific ACL enforcement in OM HA.
   * 1. Creates a mini cluster with OM HA
   * 2. Adds test user as admin to only the current leader OM node
   * 3. Validates user can perform admin operations when leader has the config
   * 4. Transfers leadership to another node (with independent configuration)
   * 5. Demonstrates that ACL enforcement depends entirely on new leader's config
   */
  @Test
  public void testOMHAAdminPrivilegesAfterLeadershipChange() throws Exception {
    // Step 1: Get the current leader OM
    OzoneManager currentLeader = cluster.getOMLeader();
    String leaderNodeId = currentLeader.getOMNodeId();
    
    // Step 2: Add test user as admin only to the current leader OM
    addAdminToSpecificOM(currentLeader, TEST_USER);
    
    // Verify admin was added
    assertTrue(currentLeader.getOmAdminUsernames().contains(TEST_USER),
        "Test user should be admin on leader OM");
    
    // Step 3: Test volume and bucket creation as test user (should succeed)
    testVolumeAndBucketCreationAsUser(true);
    
    // Step 4: Force leadership transfer to another OM node
    OzoneManager newLeader = transferLeadershipToAnotherNode(currentLeader);
    assertNotEquals(leaderNodeId, newLeader.getOMNodeId(), 
        "Leadership should have transferred to a different node");
    
    // Step 5: Verify test user is NOT admin on new leader
    assertFalse(newLeader.getOmAdminUsernames().contains(TEST_USER),
        "Test user should NOT be admin on new leader OM");
    
    // Step 6: Test volume and bucket creation as test user (should fail)
    testVolumeAndBucketCreationAsUser(false);
  }

  /**
   * Sets up the OM HA cluster with node-specific admin configurations.
   */
  private void setupCluster() throws Exception {
    OzoneConfiguration conf = createBaseConfiguration();
    conf.setClass(OZONE_ACL_AUTHORIZER_CLASS, OzoneNativeAuthorizer.class,
        IAccessAuthorizer.class);
    
    // Build HA cluster
    MiniOzoneHAClusterImpl.Builder builder = MiniOzoneCluster.newHABuilder(conf);
    builder.setOMServiceId(OM_SERVICE_ID)
        .setNumOfOzoneManagers(NUM_OF_OMS)
        .setNumDatanodes(3);
    
    cluster = builder.build();
    cluster.waitForClusterToBeReady();
    
    // Create client
    client = OzoneClientFactory.getRpcClient(OM_SERVICE_ID, conf);
  }

  /**
   * Creates base configuration for the cluster.
   */
  private OzoneConfiguration createBaseConfiguration() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    
    // Enable ACL for proper permission testing
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    
    // Set current user as initial admin (needed for cluster setup)
    String currentUser = adminUserUgi.getShortUserName();
    conf.set(OZONE_ADMINISTRATORS, currentUser);
    
    return conf;
  }

  /**
   * Creates an admin volume that will be used for testing bucket creation permissions.
   * This volume is created by the admin user, so non-admin users should not be able
   * to create buckets in it.
   */
  private void createAdminVolume() throws Exception {
    ObjectStore adminObjectStore = client.getObjectStore();
    
    // Create volume as admin user
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setOwner(adminUserUgi.getShortUserName())
        .build();
    
    adminObjectStore.createVolume(ADMIN_VOLUME, volumeArgs);
  }

  /**
   * Adds a user as admin to a specific OM instance.
   * This uses reconfiguration to add the admin user.
   */
  private void addAdminToSpecificOM(OzoneManager om, String username) throws Exception {
    // Get current admin users
    String currentAdmins = String.join(",", om.getOmAdminUsernames());
    
    // Add the new user to admin list
    String newAdmins = currentAdmins + "," + username;
    
    // Reconfigure the OM to add the new admin
    om.getReconfigurationHandler().reconfigurePropertyImpl(OZONE_ADMINISTRATORS, newAdmins);
  }

  /**
   * Tests volume and bucket creation as the test user.
   * 
   * @param shouldSucceed true if operations should succeed, false if they should fail
   */
  private void testVolumeAndBucketCreationAsUser(boolean shouldSucceed) throws Exception {
    UserGroupInformation.setLoginUser(testUserUgi);
    
    try (OzoneClient userClient = OzoneClientFactory.getRpcClient(OM_SERVICE_ID, cluster.getConf())) {
      ObjectStore userObjectStore = userClient.getObjectStore();
      
      if (shouldSucceed) {
        testVolumeCreationSuccess(userObjectStore);
        testBucketCreationSuccess(userObjectStore);
      } else {
        testVolumeCreationFailure(userObjectStore);
        testBucketCreationFailure(userObjectStore);
      }
    } finally {
      UserGroupInformation.setLoginUser(adminUserUgi);
    }
  }

  /**
   * Tests successful volume creation as admin user.
   */
  private void testVolumeCreationSuccess(ObjectStore objectStore) throws IOException {
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setOwner(TEST_USER)
        .build();
    
    objectStore.createVolume(TEST_VOLUME, volumeArgs);
    OzoneVolume volume = objectStore.getVolume(TEST_VOLUME);
    assertNotNull(volume, "Volume should be created successfully");
    assertEquals(TEST_VOLUME, volume.getName());
  }

  /**
   * Tests successful bucket creation as admin user.
   */
  private void testBucketCreationSuccess(ObjectStore objectStore) throws IOException {
    OzoneVolume volume = objectStore.getVolume(TEST_VOLUME);
    BucketArgs bucketArgs = BucketArgs.newBuilder().build();
    
    volume.createBucket(TEST_BUCKET, bucketArgs);
    OzoneBucket bucket = volume.getBucket(TEST_BUCKET);
    assertNotNull(bucket, "Bucket should be created successfully");
    assertEquals(TEST_BUCKET, bucket.getName());
  }

  /**
   * Tests that volume creation fails for non-admin user.
   */
  private void testVolumeCreationFailure(ObjectStore objectStore) {
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setOwner(TEST_USER)
        .build();
    
    String newVolumeName = generateRandomName("failtest");
    OMException exception = assertThrows(OMException.class, () -> {
      objectStore.createVolume(newVolumeName, volumeArgs);
    }, "Volume creation should fail for non-admin user");
    assertEquals(PERMISSION_DENIED, exception.getResult());
  }

  /**
   * Tests that bucket creation fails for non-admin user in admin-owned volume.
   */
  private void testBucketCreationFailure(ObjectStore objectStore) throws IOException {
    if (volumeExists(objectStore, ADMIN_VOLUME)) {
      OzoneVolume adminVolume = objectStore.getVolume(ADMIN_VOLUME);
      BucketArgs bucketArgs = BucketArgs.newBuilder().build();
      String newBucketName = generateRandomName("failtest");
      
      OMException exception = assertThrows(OMException.class, () -> {
        adminVolume.createBucket(newBucketName, bucketArgs);
      }, "Bucket creation should fail for non-admin user in admin-owned volume");
      assertEquals(PERMISSION_DENIED, exception.getResult());
    }
  }

  /**
   * Tests that setTimes ACL check is enforced in preExecute and is leader-specific.
   */
  @Test
  public void testKeySetTimesAclEnforcementAfterLeadershipChange() throws Exception {
    TestResources resources = createVolumeAndBucket("keyvol", "keybucket");
    String keyName = generateRandomName("testkey");

    // Create a key as admin (so test user is NOT the owner)
    createKey(resources.getBucket(), keyName, "test data");
    
    OzoneKey key = resources.getBucket().getKey(keyName);
    assertNotNull(key, "Key should be created successfully");
    long originalMtime = key.getModificationTime().toEpochMilli();

    // Test setTimes operation with leadership change
    testAdminOperationWithLeadershipChange(
        (objectStore, userVolume, userBucket) -> {
          long newMtime = System.currentTimeMillis();
          userBucket.setTimes(keyName, newMtime, -1);
          
          // Verify the modification time was updated
          OzoneKey updatedKey = userBucket.getKey(keyName);
          assertEquals(newMtime, updatedKey.getModificationTime().toEpochMilli(),
              "Modification time should be updated by admin user");
          assertNotEquals(originalMtime, updatedKey.getModificationTime().toEpochMilli(),
              "Modification time should have changed");
        },
        (objectStore, userVolume, userBucket) -> {
          long anotherMtime = System.currentTimeMillis() + 10000;
          userBucket.setTimes(keyName, anotherMtime, -1);
        },
        "setTimes should fail for non-admin user on new leader",
        resources.getVolumeName(), resources.getBucketName()
    );
  }

  /**
   * Tests that setQuota ACL check is enforced in preExecute and is leader-specific.
   */
  @Test
  public void testVolumeSetQuotaAclEnforcementAfterLeadershipChange() throws Exception {
    String testVolume = createVolume("quotavol");

    testAdminOperationWithLeadershipChange(
        (objectStore, userVolume, userBucket) -> {
          OzoneQuota quota1 = OzoneQuota.getOzoneQuota(100L * 1024 * 1024 * 1024, 1000);
          userVolume.setQuota(quota1);
        },
        (objectStore, userVolume, userBucket) -> {
          OzoneQuota quota2 = OzoneQuota.getOzoneQuota(200L * 1024 * 1024 * 1024, 2000);
          userVolume.setQuota(quota2);
        },
        "setQuota should fail for non-admin user on new leader",
        testVolume, null
    );
  }

  /**
   * Tests that setOwner ACL check is enforced in preExecute and is leader-specific.
   */
  @Test
  public void testVolumeSetOwnerAclEnforcementAfterLeadershipChange() throws Exception {
    String testVolume = createVolume("ownervol");

    testAdminOperationWithLeadershipChange(
        (objectStore, userVolume, userBucket) -> userVolume.setOwner("newowner"),
        (objectStore, userVolume, userBucket) -> userVolume.setOwner("anothernewowner"),
        "setOwner should fail for non-admin user on new leader",
        testVolume, null
    );
  }

  /**
   * Tests that deleteVolume ACL check is enforced in preExecute and is leader-specific.
   */
  @Test
  public void testVolumeDeleteAclEnforcementAfterLeadershipChange() throws Exception {
    String testVolume1 = createVolume("delvol1");
    String testVolume2 = createVolume("delvol2");

    testAdminOperationWithLeadershipChange(
        (objectStore, userVolume, userBucket) -> objectStore.deleteVolume(testVolume1),
        (objectStore, userVolume, userBucket) -> objectStore.deleteVolume(testVolume2),
        "deleteVolume should fail for non-admin user on new leader",
        testVolume1, null
    );
  }

  /**
   * Tests that setBucketProperty ACL check is enforced in preExecute and is leader-specific.
   */
  @Test
  public void testBucketSetPropertyAclEnforcementAfterLeadershipChange() throws Exception {
    TestResources resources = createVolumeAndBucket("bucketpropvol", "bucketprop");

    testAdminOperationWithLeadershipChange(
        (objectStore, userVolume, userBucket) -> userBucket.setVersioning(true),
        (objectStore, userVolume, userBucket) -> userBucket.setVersioning(false),
        "setBucketProperty should fail for non-admin user on new leader",
        resources.getVolumeName(), resources.getBucketName()
    );
  }

  /**
   * Tests that setBucketOwner ACL check is enforced in preExecute and is leader-specific.
   */
  @Test
  public void testBucketSetOwnerAclEnforcementAfterLeadershipChange() throws Exception {
    TestResources resources = createVolumeAndBucket("bucketownervol", "bucketowner");

    testAdminOperationWithLeadershipChange(
        (objectStore, userVolume, userBucket) -> userBucket.setOwner("newowner"),
        (objectStore, userVolume, userBucket) -> userBucket.setOwner("anothernewowner"),
        "setBucketOwner should fail for non-admin user on new leader",
        resources.getVolumeName(), resources.getBucketName()
    );
  }

  /**
   * Tests that deleteBucket ACL check is enforced in preExecute and is leader-specific.
   */
  @Test
  public void testBucketDeleteAclEnforcementAfterLeadershipChange() throws Exception {
    TestResources resources1 = createVolumeAndBucket("bucketdelvol", "bucketdel1");
    String testBucket2 = generateRandomName("bucketdel2");
    OzoneVolume adminVolume = client.getObjectStore().getVolume(resources1.getVolumeName());
    adminVolume.createBucket(testBucket2, BucketArgs.newBuilder().build());

    testAdminOperationWithLeadershipChange(
        (objectStore, userVolume, userBucket) -> userVolume.deleteBucket(resources1.getBucketName()),
        (objectStore, userVolume, userBucket) -> userVolume.deleteBucket(testBucket2),
        "deleteBucket should fail for non-admin user on new leader",
        resources1.getVolumeName(), null
    );
  }

  /**
   * Tests that deleteKeys (bulk) ACL check is enforced in preExecute and is leader-specific.
   */
  @Test
  public void testKeysDeleteAclEnforcementAfterLeadershipChange() throws Exception {
    TestResources resources = createVolumeAndBucket("keysdelvol", "keysdel");
    String keyName1 = generateRandomName("key1");
    String keyName2 = generateRandomName("key2");
    
    createKey(resources.getBucket(), keyName1, "test data");
    createKey(resources.getBucket(), keyName2, "test data");

    testAdminOperationWithLeadershipChange(
        (objectStore, userVolume, userBucket) -> userBucket.deleteKey(keyName1),
        (objectStore, userVolume, userBucket) -> userBucket.deleteKey(keyName2),
        "deleteKeys should fail for non-admin user on new leader",
        resources.getVolumeName(), resources.getBucketName()
    );
  }

  /**
   * Tests that renameKeys (bulk) ACL check is enforced in preExecute and is leader-specific.
   */
  @Test
  public void testKeysRenameAclEnforcementAfterLeadershipChange() throws Exception {
    TestResources resources = createVolumeAndBucket("keysrenamevol", "keysrename");
    String keyName1 = generateRandomName("key1");
    String keyName2 = generateRandomName("key2");
    String newKeyName1 = generateRandomName("newkey1");
    String newKeyName2 = generateRandomName("newkey2");
    
    createKey(resources.getBucket(), keyName1, "test data");
    createKey(resources.getBucket(), keyName2, "test data");

    testAdminOperationWithLeadershipChange(
        (objectStore, userVolume, userBucket) -> userBucket.renameKey(keyName1, newKeyName1),
        (objectStore, userVolume, userBucket) -> userBucket.renameKey(keyName2, newKeyName2),
        "renameKeys should fail for non-admin user on new leader",
        resources.getVolumeName(), resources.getBucketName()
    );
  }

  /**
   * Functional interface for admin operations that may throw exceptions.
   */
  @FunctionalInterface
  private interface AdminOperation {
    void execute(ObjectStore objectStore, OzoneVolume volume, OzoneBucket bucket) throws Exception;
  }

  /**
   * Helper class to hold test resource information.
   */
  private static class TestResources {
    private final String volumeName;
    private final String bucketName;
    private final OzoneVolume volume;
    private final OzoneBucket bucket;

    TestResources(String volumeName, String bucketName, OzoneVolume volume, OzoneBucket bucket) {
      this.volumeName = volumeName;
      this.bucketName = bucketName;
      this.volume = volume;
      this.bucket = bucket;
    }

    public String getVolumeName() {
      return volumeName;
    }

    public String getBucketName() {
      return bucketName;
    }

    public OzoneVolume getVolume() {
      return volume;
    }

    public OzoneBucket getBucket() {
      return bucket;
    }
  }

  /**
   * Generates a random name with a given prefix.
   */
  private String generateRandomName(String prefix) {
    return prefix + "-" + RandomStringUtils.secure().nextAlphabetic(5).toLowerCase(Locale.ROOT);
  }

  /**
   * Creates a volume with admin user.
   */
  private String createVolume(String prefix) throws IOException {
    String volumeName = generateRandomName(prefix);
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setOwner(adminUserUgi.getShortUserName())
        .build();
    client.getObjectStore().createVolume(volumeName, volumeArgs);
    return volumeName;
  }

  /**
   * Creates a volume and bucket with admin user.
   */
  private TestResources createVolumeAndBucket(String volumePrefix, String bucketPrefix) throws IOException {
    String volumeName = generateRandomName(volumePrefix);
    String bucketName = generateRandomName(bucketPrefix);
    
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setOwner(adminUserUgi.getShortUserName())
        .build();
    
    ObjectStore objectStore = client.getObjectStore();
    objectStore.createVolume(volumeName, volumeArgs);
    OzoneVolume volume = objectStore.getVolume(volumeName);
    
    BucketArgs bucketArgs = BucketArgs.newBuilder().build();
    volume.createBucket(bucketName, bucketArgs);
    OzoneBucket bucket = volume.getBucket(bucketName);
    
    return new TestResources(volumeName, bucketName, volume, bucket);
  }

  /**
   * Creates a key with the given content.
   */
  private void createKey(OzoneBucket bucket, String keyName, String content) throws IOException {
    try (OzoneOutputStream out = bucket.createKey(keyName, 0)) {
      out.write(content.getBytes(UTF_8));
    }
  }

  /**
   * Generic helper to test admin operations with leadership change.
   * This method encapsulates the common pattern:
   * 1. Add test user as admin on current leader
   * 2. Execute operation as test user (should succeed)
   * 3. Transfer leadership
   * 4. Execute operation again (should fail with PERMISSION_DENIED)
   *
   * @param successOperation operation to execute when test user is admin (should succeed)
   * @param failOperation operation to execute after leadership change (should fail)
   * @param failureMessage error message for the expected exception
   * @param volumeName volume name to access (nullable if not needed)
   * @param bucketName bucket name to access (nullable if not needed)
   */
  private void testAdminOperationWithLeadershipChange(
      AdminOperation successOperation,
      AdminOperation failOperation,
      String failureMessage,
      String volumeName,
      String bucketName) throws Exception {
    
    // Add test user as admin on current leader
    OzoneManager currentLeader = cluster.getOMLeader();
    String leaderNodeId = currentLeader.getOMNodeId();
    addAdminToSpecificOM(currentLeader, TEST_USER);
    assertTrue(currentLeader.getOmAdminUsernames().contains(TEST_USER),
        "Test user should be admin on leader OM");

    // Execute operations as test user
    UserGroupInformation.setLoginUser(testUserUgi);
    try (OzoneClient userClient = OzoneClientFactory.getRpcClient(OM_SERVICE_ID, cluster.getConf())) {
      ObjectStore userObjectStore = userClient.getObjectStore();
      OzoneVolume userVolume = volumeName != null ? userObjectStore.getVolume(volumeName) : null;
      OzoneBucket userBucket = (bucketName != null && userVolume != null) ? 
          userVolume.getBucket(bucketName) : null;

      // Execute first operation (should succeed as admin)
      successOperation.execute(userObjectStore, userVolume, userBucket);

      // Transfer leadership to another node
      OzoneManager newLeader = transferLeadershipToAnotherNode(currentLeader);
      assertNotEquals(leaderNodeId, newLeader.getOMNodeId(),
          "Leadership should have transferred to a different node");
      assertFalse(newLeader.getOmAdminUsernames().contains(TEST_USER),
          "Test user should NOT be admin on new leader OM");

      // Execute second operation (should fail on new leader)
      OMException exception = assertThrows(OMException.class, () -> {
        failOperation.execute(userObjectStore, userVolume, userBucket);
      }, failureMessage);
      assertEquals(PERMISSION_DENIED, exception.getResult(),
          "Should get PERMISSION_DENIED when ACL check fails");
    } finally {
      // Reset to original user
      UserGroupInformation.setLoginUser(adminUserUgi);
    }
  }

  /**
   * Helper method to check if volume exists.
   */
  private boolean volumeExists(ObjectStore store, String volumeName) {
    try {
      store.getVolume(volumeName);
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  /**
   * Transfers leadership from current leader to another OM node.
   * 
   * @param currentLeader the current leader OM
   * @return the new leader OM after transfer
   */
  private OzoneManager transferLeadershipToAnotherNode(OzoneManager currentLeader) throws Exception {
    // Get list of all OMs
    List<OzoneManager> omList = new ArrayList<>(cluster.getOzoneManagersList());
    
    // Remove current leader from list
    omList.remove(currentLeader);
    
    // Select the first alternative OM as target
    OzoneManager targetOM = omList.get(0);
    String targetNodeId = targetOM.getOMNodeId();
    
    // Transfer leadership
    currentLeader.transferLeadership(targetNodeId);
    
    // Wait for leadership transfer to complete
    BooleanSupplier leadershipTransferCheck = () -> {
      try {
        return !cluster.getOMLeader().getOMNodeId().equals(currentLeader.getOMNodeId());
      } catch (Exception e) {
        return false;
      }
    };
    GenericTestUtils.waitFor(leadershipTransferCheck, 1000, 30000);
    
    // Verify leadership change
    cluster.waitForLeaderOM();
    OzoneManager newLeader = cluster.getOMLeader();
    
    assertEquals(targetNodeId, newLeader.getOMNodeId(), 
        "Leadership should have transferred to target OM");
    
    return newLeader;
  }
}
