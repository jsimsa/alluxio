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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Default implementation of the {@link TypedUnderFileSystem} interface.
 */
@ThreadSafe
public class DefaultTypedUnderFileSystem<T> implements TypedUnderFileSystem<T> {
  private final UnderFileSystem mUnderFileSystem;

  public DefaultTypedUnderFileSystem(UnderFileSystem underFileSystem) {
    mUnderFileSystem = underFileSystem;
  }

  @Override
  public void close() throws IOException {
    mUnderFileSystem.close();
  }

  @Override
  public void configureProperties() throws IOException {
    mUnderFileSystem.configureProperties();
  }

  @Override
  public void connectFromMaster(String hostname) throws IOException {
    mUnderFileSystem.connectFromMaster(hostname);
  }

  @Override
  public void connectFromWorker(String hostname) throws IOException {
    mUnderFileSystem.connectFromWorker(hostname);
  }

  @Override
  public OutputStream create(T path) throws IOException {
    return mUnderFileSystem.create(path.toString());
  }

  @Override
  public OutputStream create(T path, CreateOptions options) throws IOException {
    return mUnderFileSystem.create(path.toString(), options);
  }

  @Override
  public boolean deleteDirectory(T path) throws IOException {
    return mUnderFileSystem.deleteDirectory(path.toString());
  }

  @Override
  public boolean deleteDirectory(T path, DeleteOptions options) throws IOException {
    return mUnderFileSystem.deleteDirectory(path.toString(), options);
  }

  @Override
  public boolean deleteFile(T path) throws IOException {
    return mUnderFileSystem.deleteFile(path.toString());
  }

  @Override
  public boolean exists(T path) throws IOException {
    return mUnderFileSystem.exists(path.toString());
  }

  @Override
  public long getBlockSizeByte(T path) throws IOException {
    return mUnderFileSystem.getBlockSizeByte(path.toString());
  }

  @Override
  public Object getConf() {
    return mUnderFileSystem.getConf();
  }

  @Override
  public List<String> getFileLocations(T path) throws IOException {
    return mUnderFileSystem.getFileLocations(path.toString());
  }

  @Override
  public List<String> getFileLocations(T path, FileLocationOptions options) throws IOException {
    return mUnderFileSystem.getFileLocations(path.toString(), options);
  }

  @Override
  public long getFileSize(T path) throws IOException {
    return mUnderFileSystem.getFileSize(path.toString());
  }

  @Override
  public String getGroup(T path) throws IOException {
    return mUnderFileSystem.getGroup(path.toString());
  }

  @Override
  public short getMode(T path) throws IOException {
    return mUnderFileSystem.getMode(path.toString());
  }

  @Override
  public long getModificationTimeMs(T path) throws IOException {
    return mUnderFileSystem.getModificationTimeMs(path.toString());
  }

  @Override
  public String getOwner(T path) throws IOException {
    return mUnderFileSystem.getOwner(path.toString());
  }

  @Override
  public Map<String, String> getProperties() {
    return mUnderFileSystem.getProperties();
  }

  @Override
  public long getSpace(T path, SpaceType type) throws IOException {
    return mUnderFileSystem.getSpace(path.toString(), type);
  }

  @Override
  public String getUnderFSType() {
    return mUnderFileSystem.getUnderFSType();
  }

  @Override
  public boolean isDirectory(T path) throws IOException {
    return mUnderFileSystem.isDirectory(path.toString());
  }

  @Override
  public boolean isFile(T path) throws IOException {
    return mUnderFileSystem.isFile(path.toString());
  }

  @Override
  public UnderFileStatus[] listStatus(T path) throws IOException {
    return mUnderFileSystem.listStatus(path.toString());
  }

  @Override
  public UnderFileStatus[] listStatus(T path, ListOptions options) throws IOException {
    return mUnderFileSystem.listStatus(path.toString(), options);
  }

  @Override
  public boolean mkdirs(T path) throws IOException {
    return mUnderFileSystem.mkdirs(path.toString());
  }

  @Override
  public boolean mkdirs(T path, MkdirsOptions options) throws IOException {
    return mUnderFileSystem.mkdirs(path.toString(), options);
  }

  @Override
  public InputStream open(T path) throws IOException {
    return mUnderFileSystem.open(path.toString());
  }

  @Override
  public InputStream open(T path, OpenOptions options) throws IOException {
    return mUnderFileSystem.open(path.toString(), options);
  }

  @Override
  public boolean renameDirectory(T src, T dst) throws IOException {
    return mUnderFileSystem.renameDirectory(src.toString(), dst.toString());
  }

  @Override
  public boolean renameFile(T src, T dst) throws IOException {
    return mUnderFileSystem.renameFile(src.toString(), dst.toString());
  }

  @Override
  public AlluxioURI resolveUri(AlluxioURI ufsBaseUri, String alluxioPath) {
    return mUnderFileSystem.resolveUri(ufsBaseUri, alluxioPath);
  }

  @Override
  public void setConf(Object conf) {
    mUnderFileSystem.setConf(conf);
  }

  @Override
  public void setOwner(T path, String owner, String group) throws IOException {
    mUnderFileSystem.setOwner(path.toString(), owner, group);
  }

  @Override
  public void setProperties(Map<String, String> properties) {
    mUnderFileSystem.setProperties(properties);
  }

  @Override
  public void setMode(T path, short mode) throws IOException {
    mUnderFileSystem.setMode(path.toString(), mode);
  }

  @Override
  public boolean supportsFlush() {
    return mUnderFileSystem.supportsFlush();
  }
}
