package alluxio.underfs.etcd;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Response {
  private String mAction;
  private Node mNode;
  private Node mPreviousNode;

  public Response() {}

  public String getAction() {
    return mAction;
  }

  public Node getNode() {
    return mNode;
  }

  public Node getPreviousNode() {
    return mPreviousNode;
  }

  public void setAction(String action) {
    mAction = action;
  }

  public void setNode(Node node) {
    mNode = node;
  }

  public void setPreviousNode(Node previousNode) {
    mPreviousNode = previousNode;
  }
}
