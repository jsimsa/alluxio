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

package alluxio.underfs;

import alluxio.AlluxioURI;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.FileLocationOptions;
import alluxio.underfs.options.ListOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.underfs.options.OpenOptions;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Alluxio stores data into an under layer file system. Any file system implementing this interface
 * can be a valid under layer file system
 */
@ThreadSafe
public interface TypedUnderFileSystem<T> {

  /**
   * The different types of space indicate the total space, the free space and the space used in the
   * under file system.
   */
  enum SpaceType {

    /**
     * Indicates the storage capacity of the under file system.
     */
    SPACE_TOTAL(0),

    /**
     * Indicates the amount of free space available in the under file system.
     */
    SPACE_FREE(1),

    /**
     * Indicates the amount of space used in the under file system.
     */
    SPACE_USED(2),
    ;

    private final int mValue;

    SpaceType(int value) {
      mValue = value;
    }

    /**
     * @return the integer value of this enum value
     */
    public int getValue() {
      return mValue;
    }
  }

  /**
   * Closes this under file system.
   *
   * @throws IOException if a non-Alluxio error occurs
   */
  void close() throws IOException;

  /**
   * Configures and updates the properties. For instance, this method can add new properties or
   * modify existing properties specified through {@link #setProperties(Map)}.
   *
   * The default implementation is a no-op. This should be overridden if a subclass needs
   * additional functionality.
   * @throws IOException if an error occurs during configuration
   */
  void configureProperties() throws IOException;

  /**
   * Takes any necessary actions required to establish a connection to the under file system from
   * the given master host e.g. logging in
   * <p>
   * Depending on the implementation this may be a no-op
   * </p>
   *
   * @param hostname the host that wants to connect to the under file system
   * @throws IOException if a non-Alluxio error occurs
   */
  void connectFromMaster(String hostname) throws IOException;

  /**
   * Takes any necessary actions required to establish a connection to the under file system from
   * the given worker host e.g. logging in
   * <p>
   * Depending on the implementation this may be a no-op
   * </p>
   *
   * @param hostname the host that wants to connect to the under file system
   * @throws IOException if a non-Alluxio error occurs
   */
  void connectFromWorker(String hostname) throws IOException;

  /**
   * Creates a file in the under file system with the indicated name. Parents directories will be
   * created recursively.
   *
   * @param path the file name
   * @return A {@code OutputStream} object
   * @throws IOException if a non-Alluxio error occurs
   */
  OutputStream create(T path) throws IOException;

  /**
   * Creates a file in the under file system with the specified {@link CreateOptions}.
   * Implementations should make sure that the path under creation appears in listings only
   * after a successful close and that contents are written in its entirety or not at all.
   *
   * @param path the file name
   * @param options the options for create
   * @return A {@code OutputStream} object
   * @throws IOException if a non-Alluxio error occurs
   */
  OutputStream create(T path, CreateOptions options) throws IOException;

  /**
   * Deletes a directory from the under file system with the indicated name non-recursively. A
   * non-recursive delete is successful only if the directory is empty.
   *
   * @param path of the directory to delete
   * @return true if directory was found and deleted, false otherwise
   * @throws IOException if a non-Alluxio error occurs
   */
  boolean deleteDirectory(T path) throws IOException;

  /**
   * Deletes a directory from the under file system with the indicated name.
   *
   * @param path of the directory to delete
   * @param options for directory delete semantics
   * @return true if directory was found and deleted, false otherwise
   * @throws IOException if a non-Alluxio error occurs
   */
  boolean deleteDirectory(T path, DeleteOptions options) throws IOException;

  /**
   * Deletes a file from the under file system with the indicated name.
   *
   * @param path of the file to delete
   * @return true if file was found and deleted, false otherwise
   * @throws IOException if a non-Alluxio error occurs
   */
  boolean deleteFile(T path) throws IOException;

  /**
   * Checks if a file or directory exists in under file system.
   *
   * @param path the absolute path
   * @return true if the path exists, false otherwise
   * @throws IOException if a non-Alluxio error occurs
   */
  boolean exists(T path) throws IOException;

  /**
   * Gets the block size of a file in under file system, in bytes.
   *
   * @param path the file name
   * @return the block size in bytes
   * @throws IOException if a non-Alluxio error occurs
   */
  long getBlockSizeByte(T path) throws IOException;

  /**
   * Gets the configuration object for UnderFileSystem.
   *
   * @return configuration object used for concrete ufs instance
   */
  Object getConf();

  /**
   * Gets the list of locations of the indicated path.
   *
   * @param path the file name
   * @return The list of locations
   * @throws IOException if a non-Alluxio error occurs
   */
  List<String> getFileLocations(T path) throws IOException;

  /**
   * Gets the list of locations of the indicated path given options.
   *
   * @param path the file name
   * @param options method options
   * @return The list of locations
   * @throws IOException if a non-Alluxio error occurs
   */
  List<String> getFileLocations(T path, FileLocationOptions options) throws IOException;

  /**
   * Gets the file size in bytes.
   *
   * @param path the file name
   * @return the file size in bytes
   * @throws IOException if a non-Alluxio error occurs
   */
  long getFileSize(T path) throws IOException;

  /**
   * Gets the group of the given path. An empty implementation should be provided if not supported.
   *
   * @param path the path of the file
   * @return the group of the file
   * @throws IOException if a non-Alluxio error occurs
   */
  String getGroup(T path) throws IOException;

  /**
   * Gets the mode of the given path in short format, e.g 0700. An empty implementation should
   * be provided if not supported.
   *
   * @param path the path of the file
   * @return the mode of the file
   * @throws IOException if a non-Alluxio error occurs
   */
  short getMode(T path) throws IOException;

  /**
   * Gets the UTC time of when the indicated path was modified recently in ms.
   *
   * @param path the file name
   * @return modification time in milliseconds
   * @throws IOException if a non-Alluxio error occurs
   */
  long getModificationTimeMs(T path) throws IOException;

  /**
   * Gets the owner of the given path. An empty implementation should be provided if not supported.
   *
   * @param path the path of the file
   * @return the owner of the file
   * @throws IOException if a non-Alluxio error occurs
   */
  String getOwner(T path) throws IOException;

  /**
   * @return the property map for this {@link TypedUnderFileSystem}
   */
  Map<String, String> getProperties();

  /**
   * Queries the under file system about the space of the indicated path (e.g., space left, space
   * used and etc).
   *
   * @param path the path to query
   * @param type the type of queries
   * @return The space in bytes
   * @throws IOException if a non-Alluxio error occurs
   */
  long getSpace(T path, SpaceType type) throws IOException;

  /**
   * Returns the name of the under filesystem implementation.
   *
   * The name should be lowercase and not include any spaces, e.g. "hdfs", "s3".
   *
   * @return name of the under filesystem implementation
   */
  String getUnderFSType();

  /**
   * Checks if a directory exists in under file system.
   *
   * @param path the absolute directory path
   * @return true if the path exists and is a directory, false otherwise
   * @throws IOException if a non-Alluxio error occurs
   */
  boolean isDirectory(T path) throws IOException;

  /**
   * Checks if a file exists in under file system.
   *
   * @param path the absolute file path
   * @return true if the path exists and is a file, false otherwise
   * @throws IOException if a non-Alluxio error occurs
   */
  boolean isFile(T path) throws IOException;

  /**
   * Returns an array of statuses of the files and directories in the directory denoted by this
   * abstract pathname.
   *
   * <p>
   * If this abstract pathname does not denote a directory, then this method returns {@code null}.
   * Otherwise an array of statuses is returned, one for each file or directory in the directory.
   * Names denoting the directory itself and the directory's parent directory are not included in
   * the result. Each string is a file name rather than a complete path.
   *
   * <p>
   * There is no guarantee that the name strings in the resulting array will appear in any specific
   * order; they are not, in particular, guaranteed to appear in alphabetical order.
   *
   * @param path the abstract pathname to list
   * @return An array with the statuses of the files and directories in the directory denoted by
   *         this abstract pathname. The array will be empty if the directory is empty. Returns
   *         {@code null} if this abstract pathname does not denote a directory.
   * @throws IOException if a non-Alluxio error occurs
   */
  UnderFileStatus[] listStatus(T path) throws IOException;

  /**
   * Returns an array of statuses of the files and directories in the directory denoted by this
   * abstract pathname, with options.
   *
   * <p>
   * If this abstract pathname does not denote a directory, then this method returns {@code null}.
   * Otherwise an array of statuses is returned, one for each file or directory. Names denoting the
   * directory itself and the directory's parent directory are not included in the result. Each
   * string is a path relative to the given directory.
   *
   * <p>
   * There is no guarantee that the name strings in the resulting array will appear in any specific
   * order; they are not, in particular, guaranteed to appear in alphabetical order.
   *
   * @param path the abstract pathname to list
   * @param options for list directory
   * @return An array of statuses naming the files and directories in the directory denoted by this
   *         abstract pathname. The array will be empty if the directory is empty. Returns
   *         {@code null} if this abstract pathname does not denote a directory.
   * @throws IOException if a non-Alluxio error occurs
   */
  UnderFileStatus[] listStatus(T path, ListOptions options) throws IOException;

  /**
   * Creates the directory named by this abstract pathname. If the folder already exists, the method
   * returns false. The method creates any necessary but nonexistent parent directories.
   *
   * @param path the folder to create
   * @return {@code true} if and only if the directory was created; {@code false} otherwise
   * @throws IOException if a non-Alluxio error occurs
   */
  boolean mkdirs(T path) throws IOException;

  /**
   * Creates the directory named by this abstract pathname, with specified
   * {@link MkdirsOptions}. If the folder already exists, the method returns false.
   *
   * @param path the folder to create
   * @param options the options for mkdirs
   * @return {@code true} if and only if the directory was created; {@code false} otherwise
   * @throws IOException if a non-Alluxio error occurs
   */
  boolean mkdirs(T path, MkdirsOptions options) throws IOException;

  /**
   * Opens an {@link InputStream} at the indicated path.
   *
   * @param path the file name
   * @return The {@code InputStream} object
   * @throws IOException if a non-Alluxio error occurs
   */
  InputStream open(T path) throws IOException;

  /**
   * Opens an {@link InputStream} at the indicated path.
   *
   * @param path the file name
   * @param options to open input stream
   * @return The {@code InputStream} object
   * @throws IOException if a non-Alluxio error occurs
   */
  InputStream open(T path, OpenOptions options) throws IOException;

  /**
   * Renames a directory from {@code src} to {@code dst} in under file system.
   *
   * @param src the source directory path
   * @param dst the destination directory path
   * @return true if succeed, false otherwise
   * @throws IOException if a non-Alluxio error occurs
   */
  boolean renameDirectory(T src, T dst) throws IOException;

  /**
   * Renames a file from {@code src} to {@code dst} in under file system.
   *
   * @param src the source file path
   * @param dst the destination file path
   * @return true if succeed, false otherwise
   * @throws IOException if a non-Alluxio error occurs
   */
  boolean renameFile(T src, T dst) throws IOException;

  /**
   * Returns an {@link AlluxioURI} representation for the {@link TypedUnderFileSystem} given a base
   * UFS URI, and the Alluxio path from the base.
   *
   * The default implementation simply concatenates the path to the base URI. This should be
   * overridden if a subclass needs alternate functionality.
   *
   * @param ufsBaseUri the base {@link AlluxioURI} in the ufs
   * @param alluxioPath the path in Alluxio from the given base
   * @return the UFS {@link AlluxioURI} representing the Alluxio path
   */
  AlluxioURI resolveUri(AlluxioURI ufsBaseUri, String alluxioPath);

  /**
   * Sets the configuration object for UnderFileSystem. The conf object is understood by the
   * concrete underfs's implementation.
   *
   * @param conf the configuration object accepted by ufs
   */
  void setConf(Object conf);

  /**
   * Sets the user and group of the given path. An empty implementation should be provided if
   * unsupported.
   *
   * @param path the path of the file
   * @param owner the new owner to set, unchanged if null
   * @param group the new group to set, unchanged if null
   * @throws IOException if a non-Alluxio error occurs
   */
  void setOwner(T path, String owner, String group) throws IOException;

  /**
   * Sets the properties for this {@link TypedUnderFileSystem}.
   *
   * @param properties a {@link Map} of property names to values
   */
  void setProperties(Map<String, String> properties);

  /**
   * Changes posix file mode.
   *
   * @param path the path of the file
   * @param mode the mode to set in short format, e.g. 0777
   * @throws IOException if a non-Alluxio error occurs
   */
  void setMode(T path, short mode) throws IOException;

  /**
   * Whether this type of UFS supports flush.
   *
   * @return true if this type of UFS supports flush, false otherwise
   */
  boolean supportsFlush();
}
