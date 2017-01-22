/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs.etcd;

import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.UnderFileSystemRegistry;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link EtcdUnderFileSystemFactory}.
 */
public class EtcdUnderFileSystemFactoryTest {

  /**
   * This test ensures the etcd UFS module correctly accepts paths that begin with etcd://.
   */
  @Test
  public void factory() {
    UnderFileSystemFactory factory = UnderFileSystemRegistry.find("/local/test/path");
    UnderFileSystemFactory factory2 = UnderFileSystemRegistry.find("file://local/test/path");
    UnderFileSystemFactory factory3 = UnderFileSystemRegistry.find("etcd://hostname:port/path");
    UnderFileSystemFactory factory4 = UnderFileSystemRegistry.find("hdfs://hostname:port/path");

    Assert.assertNull(
        "A UnderFileSystemFactory should not exist for local paths when using this module",
        factory);
    Assert.assertNull(
        "A UnderFileSystemFactory should not exist for local paths when using this module",
        factory2);
    Assert.assertNotNull(
        "A UnderFileSystemFactory should not exist for etcd paths when using this module",
        factory3);
    Assert.assertNull(
        "A UnderFileSystemFactory should not exist for HDFS paths when using this module",
        factory4);
  }
}
