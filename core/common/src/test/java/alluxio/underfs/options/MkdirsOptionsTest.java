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

package alluxio.underfs.options;

import alluxio.CommonTestUtils;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.security.authentication.AuthType;
import alluxio.security.authorization.Mode;
import alluxio.security.group.provider.IdentityUserGroupsMapping;
import alluxio.util.CommonUtils;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.Random;

/**
 * Tests for the {@link MkdirsOptions} class.
 */
public final class MkdirsOptionsTest {
  /**
   * Tests for default {@link MkdirsOptions}.
   */
  @Test
  public void defaults() throws IOException {
    MkdirsOptions options = MkdirsOptions.defaults();

    // Verify the default createParent is true.
    Assert.assertTrue(options.getCreateParent());
    // Verify that the owner and group are not set.
    Assert.assertEquals("", options.getOwner());
    Assert.assertEquals("", options.getGroup());
    Assert.assertEquals(Mode.defaults().applyDirectoryUMask(), options.getMode());
  }

  /**
   * Tests for building an {@link MkdirsOptions} with a security enabled
   * configuration.
   */
  @Test
  public void securityEnabled() throws IOException {
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    Configuration.set(PropertyKey.SECURITY_LOGIN_USERNAME, "foo");
    // Use IdentityUserGroupMapping to map user "foo" to group "foo".
    Configuration.set(PropertyKey.SECURITY_GROUP_MAPPING_CLASS,
        IdentityUserGroupsMapping.class.getName());

    MkdirsOptions options = MkdirsOptions.defaults();

    // Verify the default createParent is true.
    Assert.assertTrue(options.getCreateParent());
    // Verify that the owner and group are not set.
    Assert.assertEquals("", options.getOwner());
    Assert.assertEquals("", options.getGroup());
    Assert.assertEquals(Mode.defaults().applyDirectoryUMask(), options.getMode());
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() {
    Random random = new Random();
    boolean createParent = random.nextBoolean();
    String owner = CommonUtils.randomAlphaNumString(10);
    String group = CommonUtils.randomAlphaNumString(10);
    Mode mode = new Mode((short) random.nextInt());

    MkdirsOptions options = MkdirsOptions.defaults();
    options.setCreateParent(createParent);
    options.setOwner(owner);
    options.setGroup(group);
    options.setMode(mode);

    Assert.assertEquals(createParent, options.getCreateParent());
    Assert.assertEquals(owner, options.getOwner());
    Assert.assertEquals(group, options.getGroup());
    Assert.assertEquals(mode, options.getMode());
  }

  @Test
  public void equalsTest() throws Exception {
    CommonTestUtils.testEquals(MkdirsOptions.class);
  }
}
