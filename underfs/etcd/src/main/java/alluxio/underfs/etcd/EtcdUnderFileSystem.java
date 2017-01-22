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

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.underfs.AtomicFileOutputStreamCallback;
import alluxio.underfs.BaseUnderFileSystem;
import alluxio.underfs.UnderFileStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.FileLocationOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.underfs.options.OpenOptions;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Etcd FS {@link UnderFileSystem} implementation.
 */
@ThreadSafe
public class EtcdUnderFileSystem extends BaseUnderFileSystem
    implements AtomicFileOutputStreamCallback {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static final String PROTOCOL = "http";
  private static final String HOSTNAME = "127.0.0.1";
  private static final int PORT = 2379;

  private static String createURL(String path) throws IOException {
    return new URL(PROTOCOL, HOSTNAME, PORT, "/v2/keys" + stripPath(path)).toString();
  }

  /**
   * Constructs a new {@link EtcdUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   */
  public EtcdUnderFileSystem(AlluxioURI uri) {
    super(uri);
  }

  @Override
  public String getUnderFSType() {
    return "etcd";
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public OutputStream create(String path, CreateOptions options) throws IOException {
    HttpClient client = HttpClientBuilder.create().build();
    HttpPut request = new HttpPut(createURL(path));
    HttpResponse response = client.execute(request);
    BufferedReader rd =
        new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
    StringBuilder responseBody = new StringBuilder();
    String line;
    while ((line = rd.readLine()) != null) {
      responseBody.append(line);
    }
    LOG.info(responseBody.toString());
    return new EtcdOutputStream();
  }

  @Override
  public OutputStream createDirect(String path, CreateOptions options) throws IOException {
    throw new IOException("not implemented");
  }

  @Override
  public boolean deleteDirectory(String path, DeleteOptions options) throws IOException {
    HttpClient client = HttpClientBuilder.create().build();
    HttpDelete request = new HttpDelete(createURL(path)+"?recursive="+options.isRecursive());
    HttpResponse response = client.execute(request);
    if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
      return false;
    }
    BufferedReader rd =
        new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
    StringBuilder responseBody = new StringBuilder();
    String line;
    while ((line = rd.readLine()) != null) {
      responseBody.append(line);
    }
    LOG.info(responseBody.toString());
    return true;
  }

  @Override
  public boolean deleteFile(String path) throws IOException {
    HttpClient client = HttpClientBuilder.create().build();
    HttpDelete request = new HttpDelete(createURL(path));
    HttpResponse response = client.execute(request);
    if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
      return false;
    }
    BufferedReader rd =
        new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
    StringBuilder responseBody = new StringBuilder();
    String line;
    while ((line = rd.readLine()) != null) {
      responseBody.append(line);
    }
    LOG.info(responseBody.toString());
    return true;
  }

  @Override
  public boolean exists(String path) throws IOException {
    throw new IOException("not implemented");
  }

  @Override
  public long getBlockSizeByte(String path) throws IOException {
    throw new IOException("not implemented");
  }

  @Override
  public Object getConf() {
    return null;
  }

  @Override
  public List<String> getFileLocations(String path) throws IOException {
    throw new IOException("not implemented");
  }

  @Override
  public List<String> getFileLocations(String path, FileLocationOptions options)
      throws IOException {
    throw new IOException("not implemented");
  }

  @Override
  public long getFileSize(String path) throws IOException {
    throw new IOException("not implemented");
  }

  @Override
  public long getModificationTimeMs(String path) throws IOException {
    throw new IOException("not implemented");
  }

  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
    throw new IOException("not implemented");
  }

  @Override
  public boolean isDirectory(String path) throws IOException {
    HttpClient client = HttpClientBuilder.create().build();
    HttpGet request = new HttpGet(createURL(path));
    HttpResponse response = client.execute(request);
    if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
      return false;
    }
    BufferedReader rd =
        new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
    StringBuilder responseBody = new StringBuilder();
    String line;
    while ((line = rd.readLine()) != null) {
      responseBody.append(line);
    }
    LOG.info(responseBody.toString());
    ObjectMapper mapper = new ObjectMapper();
    Node node = mapper.readValue(responseBody.toString(), Response.class).getNode();
    return node.isDir();
  }

  @Override
  public boolean isFile(String path) throws IOException {
    HttpClient client = HttpClientBuilder.create().build();
    HttpGet request = new HttpGet(createURL(path));
    HttpResponse response = client.execute(request);
    if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
      return false;
    }
    BufferedReader rd =
        new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
    StringBuilder responseBody = new StringBuilder();
    String line;
    while ((line = rd.readLine()) != null) {
      responseBody.append(line);
    }
    LOG.info(responseBody.toString());
    ObjectMapper mapper = new ObjectMapper();
    Node node = mapper.readValue(responseBody.toString(), Response.class).getNode();
    return !node.isDir();
  }

  @Override
  public UnderFileStatus[] listStatus(String path) throws IOException {
    HttpClient client = HttpClientBuilder.create().build();
    HttpGet request = new HttpGet(createURL(path));
    HttpResponse response = client.execute(request);
    BufferedReader rd =
        new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
    StringBuilder responseBody = new StringBuilder();
    String line;
    while ((line = rd.readLine()) != null) {
      responseBody.append(line);
    }
    ObjectMapper mapper = new ObjectMapper();
    Node node = mapper.readValue(responseBody.toString(), Response.class).getNode();
    List<Node> nodes = node.getNodes();
    if (nodes == null) {
      return new UnderFileStatus[0];
    }
    UnderFileStatus[] result = new UnderFileStatus[nodes.size()];
    for (int i = 0; i < nodes.size(); i++) {
      Node n = nodes.get(i);
      LOG.info(new File(n.getKey()).getName() + " " + n.isDir());
      result[i] = new UnderFileStatus(new File(n.getKey()).getName(), n.isDir());
    }
    return result;
  }

  @Override
  public boolean mkdirs(String path, MkdirsOptions options) throws IOException {
    HttpClient client = HttpClientBuilder.create().build();
    HttpPut request = new HttpPut(createURL(path));
    List<NameValuePair> urlParameters = new ArrayList<>();
    urlParameters.add(new BasicNameValuePair("dir", "true"));
    request.setEntity(new UrlEncodedFormEntity(urlParameters));
    HttpResponse response = client.execute(request);
    BufferedReader rd =
        new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
    StringBuilder responseBody = new StringBuilder();
    String line;
    while ((line = rd.readLine()) != null) {
      responseBody.append(line);
    }
    LOG.info(responseBody.toString());
    return true;
  }

  @Override
  public InputStream open(String path, OpenOptions options) throws IOException {
    throw new IOException("not implemented");
  }

  @Override
  public boolean renameDirectory(String src, String dst) throws IOException {
    throw new IOException("not implemented");
  }

  @Override
  public boolean renameFile(String src, String dst) throws IOException {
    // TODO(jiri) implement
    return true;
  }

  @Override
  public void setConf(Object conf) {}

  @Override
  public void setOwner(String path, String user, String group) throws IOException {
    throw new IOException("not implemented");
  }

  @Override
  public void setMode(String path, short mode) throws IOException {
    throw new IOException("not implemented");
  }

  @Override
  public String getOwner(String path) throws IOException {
    throw new IOException("not implemented");
  }

  @Override
  public String getGroup(String path) throws IOException {
    throw new IOException("not implemented");
  }

  @Override
  public short getMode(String path) throws IOException {
    throw new IOException("not implemented");
  }

  @Override
  public void connectFromMaster(String hostname) throws IOException {
    // No-op
  }

  @Override
  public void connectFromWorker(String hostname) throws IOException {
    // No-op
  }

  @Override
  public boolean supportsFlush() {
    return true;
  }

  /**
   * @param path the path to strip the scheme from
   * @return the path, with the optional scheme stripped away
   */
  private static String stripPath(String path) {
    return new AlluxioURI(path).getPath();
  }
}
