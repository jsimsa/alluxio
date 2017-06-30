package alluxio.master.permission;

import alluxio.Constants;

import com.google.common.base.Objects;

public class PosixPermission {
  private String mGroup;
  private String mOwner;
  private short mMode;

  public PosixPermission() {
    mGroup = "";
    mMode = Constants.INVALID_MODE;
    mOwner = "";
  }

  /**
   * @return the group
   */
  public String getGroup() {
    return mGroup;
  }

  /**
   * @return the mode
   */
  public short getMode() {
    return mMode;
  }

  /**
   * @return the owner
   */
  public String getOwner() {
    return mOwner;
  }

  /**
   * @param group the group
   * @return the updated object
   */
  public PosixPermission setGroup(String group) {
    mGroup = group;
    return this;
  }

  /**
   * @param mode the mode
   * @return the updated object
   */
  public PosixPermission setMode(short mode) {
    mMode = mode;
    return this;
  }

  /**
   * @param owner the owner name
   * @return the updated object
   */
  public PosixPermission setOwner(String owner) {
    mOwner = owner;
    return this;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PosixPermission)) {
      return false;
    }
    PosixPermission that = (PosixPermission) o;
    return Objects.equal(mGroup, that.mGroup)
        && Objects.equal(mOwner, that.mOwner)
        && Objects.equal(mMode, that.mMode);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mGroup, mMode, mOwner);
  }

  protected Objects.ToStringHelper toStringHelper() {
    return Objects.toStringHelper(this).add("owner", mOwner).add("group", mGroup)
        .add("mode", mMode);
  }
}
