package alluxio.master.permission;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.clock.Clock;
import alluxio.clock.SystemClock;
import alluxio.exception.AccessControlException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidPathException;
import alluxio.exception.PreconditionMessage;
import alluxio.master.AbstractMaster;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.master.journal.JournalFactory;
import alluxio.proto.journal.Journal;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.authorization.Mode;
import alluxio.util.CommonUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.executor.ExecutorServiceFactory;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;
import org.apache.thrift.TProcessor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DefaultPermissionMaster extends AbstractMaster implements PermissionMaster {
  /** Whether the permission check is enabled. */
  private final boolean mPermissionCheckEnabled;

  /** The super user of Alluxio file system. */
  private final String mFileSystemSuperUser;

  /** The super group of Alluxio file system. All users in this group have super permission. */
  private final String mFileSystemSuperGroup;

  private final Map<Long, PosixPermission> mPermissions;


  /**
   * Creates a new instance of {@link DefaultPermissionMaster}.
   *
   * @param journalFactory the factory for the journal to use for tracking master operations
   */
  DefaultPermissionMaster(JournalFactory journalFactory) {
    this(journalFactory, new SystemClock(), ExecutorServiceFactories
        .fixedThreadPoolExecutorServiceFactory(Constants.PERMISSION_MASTER_NAME, 2));
  }

  /**
   * Creates a new instance of {@link DefaultPermissionMaster}.
   *
   * @param journalFactory the factory for the journal to use for tracking master operations
   * @param clock the clock to use for determining the time
   * @param executorServiceFactory a factory for creating the executor service to use for running
   *        maintenance threads
   */
  DefaultPermissionMaster(JournalFactory journalFactory, Clock clock,
      ExecutorServiceFactory executorServiceFactory) {
    super(journalFactory.create(Constants.PERMISSION_MASTER_NAME), clock, executorServiceFactory);
    mPermissions = new HashMap<>();
    mPermissionCheckEnabled =
        Configuration.getBoolean(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED);
    mFileSystemSuperGroup =
        Configuration.get(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP);
    mFileSystemSuperUser =
        Configuration.get(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_SUPERUSER);
  }

  @Override
  public String getName() {
    return Constants.PERMISSION_MASTER_NAME;
  }

  @Override
  public void processJournalEntry(Journal.JournalEntry entry) throws IOException {
  }

  @Override
  public Iterator<Journal.JournalEntry> getJournalEntryIterator() {
    return null;
  }

  @Override
  public Map<String, TProcessor> getServices() {
    return new HashMap<>();
  }

  @Override
  public String getGroup(long fileId) {
    return mPermissions.get(fileId).getGroup();
  }

  @Override
  public short getMode(long fileId) {
    return mPermissions.get(fileId).getMode();
  }

  @Override
  public String getOwner(long fileId) {
    return mPermissions.get(fileId).getOwner();
  }

  @Override
  public void setGroup(long fileId, String group) {
    PosixPermission p = mPermissions.get(fileId);
    if (p == null) {
      p = new PosixPermission();
    }
    p.setGroup(group);
    // TODO(jiri): journal
  }

  @Override
  public void setMode(long fileId, short mode) {
    PosixPermission p = mPermissions.get(fileId);
    if (p == null) {
      p = new PosixPermission();
    }
    p.setMode(mode);
    // TODO(jiri): journal
  }

  @Override
  public void setOwner(long fileId, String owner) {
    PosixPermission p = mPermissions.get(fileId);
    if (p == null) {
      p = new PosixPermission();
    }
    p.setOwner(owner);
    // TODO(jiri): journal
  }

  @Override
  public void checkParentPermission(Mode.Bits bits, LockedInodePath inodePath)
      throws AccessControlException, InvalidPathException {
    if (!mPermissionCheckEnabled) {
      return;
    }

    // root "/" has no parent, so return without checking
    if (PathUtils.isRoot(inodePath.getUri().getPath())) {
      return;
    }

    // collects existing inodes info on the path. Note that, not all the components of the path have
    // corresponding inodes.
    List<Inode<?>> inodeList = inodePath.getInodeList();

    // collects user and groups
    String user = AuthenticatedClientUser.getClientUser();
    List<String> groups = getGroups(user);

    // remove the last element if all components of the path exist, since we only check the parent.
    if (inodePath.fullPathExists()) {
      inodeList.remove(inodeList.size() - 1);
    }
    checkInodeList(user, groups, bits, inodePath.getUri().getPath(), inodeList, false);
  }

  @Override
  public void checkPermission(Mode.Bits bits, LockedInodePath inodePath)
      throws AccessControlException, InvalidPathException {
    if (!mPermissionCheckEnabled) {
      return;
    }

    // collects inodes info on the path
    List<Inode<?>> inodeList = inodePath.getInodeList();

    // collects user and groups
    String user = AuthenticatedClientUser.getClientUser();
    List<String> groups = getGroups(user);

    checkInodeList(user, groups, bits, inodePath.getUri().getPath(), inodeList, false);
  }

  @Override
  public Mode.Bits getPermission(LockedInodePath inodePath) {
    if (!mPermissionCheckEnabled) {
      return Mode.Bits.NONE;
    }
    // collects inodes info on the path
    List<Inode<?>> inodeList = inodePath.getInodeList();

    // collects user and groups
    try {
      String user = AuthenticatedClientUser.getClientUser();
      List<String> groups = getGroups(user);
      return getPermissionInternal(user, groups, inodePath.getUri().getPath(), inodeList);
    } catch (AccessControlException e) {
      return Mode.Bits.NONE;
    }
  }

  @Override
  public void checkSetAttributePermission(LockedInodePath inodePath, boolean superuserRequired,
      boolean ownerRequired) throws AccessControlException, InvalidPathException {
    if (!mPermissionCheckEnabled) {
      return;
    }

    // For chown, superuser is required
    if (superuserRequired) {
      checkSuperUser();
    }
    // For chgrp or chmod, owner or superuser (supergroup) is required
    if (ownerRequired) {
      checkOwner(inodePath);
    }
    checkPermission(Mode.Bits.WRITE, inodePath);
  }

  /**
   * @param user the user to get groups for
   * @return the groups for the given user
   * @throws AccessControlException if the group service information cannot be accessed
   */
  private List<String> getGroups(String user) throws AccessControlException {
    try {
      return CommonUtils.getGroups(user);
    } catch (IOException e) {
      throw new AccessControlException(
          ExceptionMessage.PERMISSION_DENIED.getMessage(e.getMessage()));
    }
  }

  /**
   * Checks whether the client user is the owner of the path.
   *
   * @param inodePath path to be checked on
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the path is invalid
   */
  private void checkOwner(LockedInodePath inodePath)
      throws AccessControlException, InvalidPathException {
    // collects inodes info on the path
    List<Inode<?>> inodeList = inodePath.getInodeList();

    // collects user and groups
    String user = AuthenticatedClientUser.getClientUser();
    List<String> groups = getGroups(user);

    if (isPrivilegedUser(user, groups)) {
      return;
    }

    checkInodeList(user, groups, null, inodePath.getUri().getPath(), inodeList, true);
  }

  /**
   * Checks whether the user is a super user or in super group.
   *
   * @throws AccessControlException if the user is not a super user
   */
  private void checkSuperUser() throws AccessControlException {
    // collects user and groups
    String user = AuthenticatedClientUser.getClientUser();
    List<String> groups = getGroups(user);
    if (!isPrivilegedUser(user, groups)) {
      throw new AccessControlException(ExceptionMessage.PERMISSION_DENIED
          .getMessage(user + " is not a super user or in super group"));
    }
  }

  /**
   * This method provides basic permission checking logic on a list of inodes. The input includes
   * user and its group, requested action and inode list (by traversing the path). Then user,
   * group, and the requested action will be evaluated on each of the inodes. It will return if
   * check passed, and throw exception if check failed.
   *
   * @param user who requests access permission
   * @param groups in which user belongs to
   * @param bits bits that capture the action {@link Mode.Bits} by user
   * @param path the path to check permission on
   * @param inodeList file info list of all the inodes retrieved by traversing the path
   * @param checkIsOwner indicates whether to check the user is the owner of the path
   * @throws AccessControlException if permission checking fails
   */
  private void checkInodeList(String user, List<String> groups, Mode.Bits bits,
      String path, List<Inode<?>> inodeList, boolean checkIsOwner) throws AccessControlException {
    int size = inodeList.size();
    Preconditions
        .checkArgument(size > 0, PreconditionMessage.EMPTY_FILE_INFO_LIST_FOR_PERMISSION_CHECK);

    // bypass checking permission for super user or super group of Alluxio file system.
    if (isPrivilegedUser(user, groups)) {
      return;
    }

    // traverses from root to the parent dir to all inodes included by this path are executable
    for (int i = 0; i < size - 1; i++) {
      checkInode(user, groups, inodeList.get(i), Mode.Bits.EXECUTE, path);
    }

    Inode inode = inodeList.get(inodeList.size() - 1);
    if (checkIsOwner) {
      if (inode == null || user.equals(getOwner(inode.getId()))) {
        return;
      }
      throw new AccessControlException(ExceptionMessage.PERMISSION_DENIED
          .getMessage("user=" + user + " is not the owner of path=" + path));
    }
    checkInode(user, groups, inode, bits, path);
  }

  /**
   * This method checks requested permission on a given inode, represented by its fileInfo.
   *
   * @param user who requests access permission
   * @param groups in which user belongs to
   * @param inode whose attributes used for permission check logic
   * @param bits requested {@link Mode.Bits} by user
   * @param path the path to check permission on
   * @throws AccessControlException if permission checking fails
   */
  private void checkInode(String user, List<String> groups, Inode<?> inode, Mode.Bits bits,
      String path) throws AccessControlException {
    if (inode == null) {
      return;
    }
    long fileId = inode.getId();
    short mode = getMode(fileId);
    if (user.equals(getOwner(fileId)) && Mode.extractOwnerBits(mode).imply(bits)) {
      return;
    }
    if (groups.contains(getGroup(fileId)) && Mode.extractGroupBits(mode).imply(bits)) {
      return;
    }
    if (Mode.extractOtherBits(mode).imply(bits)) {
      return;
    }
    throw new AccessControlException(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(user, bits, path, inode, getOwner(fileId), getGroup(fileId), mode)));
  }

  /**
   * Gets the permission to access an inode path given a user and its groups.
   *
   * @param user the user
   * @param groups the groups this user belongs to
   * @param path the inode path
   * @param inodeList the list of inodes in the path
   * @return the permission
   */
  private Mode.Bits getPermissionInternal(String user, List<String> groups, String path,
      List<Inode<?>> inodeList) {
    int size = inodeList.size();
    Preconditions
        .checkArgument(size > 0, PreconditionMessage.EMPTY_FILE_INFO_LIST_FOR_PERMISSION_CHECK);

    // bypass checking permission for super user or super group of Alluxio file system.
    if (isPrivilegedUser(user, groups)) {
      return Mode.Bits.ALL;
    }

    // traverses from root to the parent dir to all inodes included by this path are executable
    for (int i = 0; i < size - 1; i++) {
      try {
        checkInode(user, groups, inodeList.get(i), Mode.Bits.EXECUTE, path);
      } catch (AccessControlException e) {
        return Mode.Bits.NONE;
      }
    }

    Inode inode = inodeList.get(inodeList.size() - 1);
    if (inode == null) {
      return Mode.Bits.NONE;
    }

    Mode.Bits mode = Mode.Bits.NONE;
    long fileId = inode.getId();
    short permission = getMode(fileId);
    if (user.equals(getOwner(fileId))) {
      mode = mode.or(Mode.extractOwnerBits(permission));
    }
    if (groups.contains(getGroup(fileId))) {
      mode = mode.or(Mode.extractGroupBits(permission));
    }
    mode = mode.or(Mode.extractOtherBits(permission));
    return mode;
  }

  private boolean isPrivilegedUser(String user, List<String> groups) {
    return user.equals(mFileSystemSuperUser) || groups.contains(mFileSystemSuperGroup);
  }

  private static String toExceptionMessage(String user, Mode.Bits bits, String path,
      Inode<?> inode, String owner, String group, short mode) {
    return new StringBuilder().append("user=").append(user).append(", ").append("access=")
        .append(bits).append(", ").append("path=").append(path).append(": ").append("failed at ")
        .append(inode.getName().equals("") ? "/" : inode.getName()).append(", inode owner=")
        .append(owner).append(", inode group=").append(group).append(", inode mode=")
        .append(new Mode(mode)).toString();
  }
}
