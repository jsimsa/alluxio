package alluxio.master.permission;

import alluxio.exception.AccessControlException;
import alluxio.exception.InvalidPathException;
import alluxio.master.Master;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.master.file.options.CreatePathOptions;
import alluxio.security.authorization.Mode;

public interface PermissionMaster extends Master {
  /**
   * Checks whether a user has permission to perform a specific action on the parent of the given
   * path; if parent directory does not exist, treats the closest ancestor directory of the path as
   * its parent and checks permission on it. This check will pass if the path is invalid, or path
   * has no parent (e.g., root).
   *
   * @param bits bits that capture the action {@link Mode.Bits} by user
   * @param inodePath the path to check permission on
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the path is invalid
   */
  void checkParentPermission(Mode.Bits bits, LockedInodePath inodePath)
      throws AccessControlException, InvalidPathException;

  /**
   * Checks whether a user has permission to perform a specific action on a path. This check will
   * pass if the path is invalid.
   *
   * @param bits bits that capture the action {@link Mode.Bits} by user
   * @param inodePath the path to check permission on
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the path is invalid
   */
  void checkPermission(Mode.Bits bits, LockedInodePath inodePath)
      throws AccessControlException, InvalidPathException;

  /**
   * Checks whether a user has permission to edit the attribute of a given path.
   *
   * @param inodePath the path to check permission on
   * @param superuserRequired indicates whether it requires to be the superuser
   * @param ownerRequired indicates whether it requires to be the owner of this path
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the path is invalid
   */
  void checkSetAttributePermission(LockedInodePath inodePath, boolean superuserRequired,
      boolean ownerRequired) throws AccessControlException, InvalidPathException;
  /**
   * Gets the permission to access inodePath for the current client user.
   *
   * @param inodePath the inode path
   * @return the permission
   */
  Mode.Bits getPermission(LockedInodePath inodePath);

  void create(long fileId, CreatePathOptions options);

  String getGroup(long fileId);
  short getMode(long fileId);
  String getOwner(long fileId);

  void setGroup(long fileId, String group);
  void setMode(long fileId, short mode);
  void setOwner(long fileId, String owner);
}
