import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class WatchersDemo implements Watcher {
  private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
  private static final int SESSION_TIMEOUT = 3000;
  private static final String WATCHED_ZNODE = "/watched_znode";
  private ZooKeeper zooKeeper;
  private String zoomoji;

  public WatchersDemo() {
    this.zoomoji = PscottUtils.zoomoji();
  }

  public static void main(String[] args) throws InterruptedException, IOException, KeeperException {
    WatchersDemo watchersDemo = new WatchersDemo();
    watchersDemo.connectToZookeeper();
    watchersDemo.watchZnode();
    watchersDemo.run();
    watchersDemo.close();

    System.out.println("👋 Disconnected from ZooKeeper");
  }

  public void connectToZookeeper() throws IOException {
    this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
  }

  public void run() throws InterruptedException {
    synchronized (zooKeeper) {
      zooKeeper.wait();
    }
  }

  public void close() throws InterruptedException {
    zooKeeper.close();
  }

  public void watchZnode() throws KeeperException, InterruptedException {
    // Exists sets up a watch for creation/deletion of the node, or data set on it.
    Stat stat = zooKeeper.exists(WATCHED_ZNODE, this);

    // If znode to watch doesn't exist, .exists returns null
    if (Objects.isNull(stat)) {
      return;
    }

    // Get data and establish a watch, which will fire by an operation that sets data on the watched
    // node, or deletes it.
    byte[] data = zooKeeper.getData(WATCHED_ZNODE, this, stat);
    List<String> children = zooKeeper.getChildren(WATCHED_ZNODE, this);

    System.out.println(zoomoji + " sees data: " + new String(data) + ", children: " + children);
  }

  @Override
  public void process(WatchedEvent watchedEvent) {
    switch (watchedEvent.getType()) {
      // General zk events don't have any type
      case None:
        if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
          System.out.println(zoomoji + " Connected to ZooKeeper");
        } else {
          synchronized (zooKeeper) {
            System.out.println(zoomoji + " Received disconnected from ZooKeeper event");
            zooKeeper.notifyAll();
          }
        }
      case NodeDeleted:
        System.out.println(zoomoji + " sees " + WATCHED_ZNODE + " was deleted");
        break;
      case NodeCreated:
        System.out.println(zoomoji + " sees " + WATCHED_ZNODE + " was created");
        break;
      case NodeDataChanged:
        System.out.println(zoomoji + " sees " + WATCHED_ZNODE + " data changed");
        break;
      case NodeChildrenChanged:
        System.out.println(zoomoji + " sees " + WATCHED_ZNODE + " children changed");
        break;
    }

    try {
      watchZnode();
    } catch (KeeperException | InterruptedException ignored) {
    }
  }
}
