package alluxio.underfs.etcd;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Node {
  private String mKey;
  private boolean mDir;
  private String mValue;
  private List<Node> mNodes;

  public Node() {}

  public String getKey() {
    return mKey;
  }

  public boolean isDir() {
    return mDir;
  }

  public String getValue() {
    return mValue;
  }

  public List<Node> getNodes() {
    return mNodes;
  }

  public void setKey(String key) {
    mKey = key;
  }

  public void setDir(boolean dir) {
    mDir = dir;
  }

  public void setValue(String value) {
    mValue = value;
  }

  public void setNodes(List<Node> nodes) {
    mNodes = nodes;
  }
}

