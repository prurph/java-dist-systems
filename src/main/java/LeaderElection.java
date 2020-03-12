import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

public class LeaderElection implements Watcher {
  private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
  private static final int SESSION_TIMEOUT = 3000;
  private ZooKeeper zooKeeper;

  public static void main(String[] args) throws InterruptedException, IOException {
    LeaderElection leaderElection = new LeaderElection();

    leaderElection.connectToZookeeper();
    leaderElection.run();
    leaderElection.close();

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

  public void process(WatchedEvent watchedEvent) {
    switch (watchedEvent.getType()) {
      // General zk events don't have any type
      case None:
        if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
          System.out.println(getZoomoji() + " Connected to ZooKeeper");
        } else {
          synchronized (zooKeeper) {
            System.out.println(getZoomoji() + " Received disconnected from ZooKeeper event");
            zooKeeper.notifyAll();
          }
        }
    }
  }

  private String getZoomoji() {
    String[] zoomojis = {"🦁", "🐯", "🐵", "🐨", "🐻", "🦥", "🦜"};
    return zoomojis[ThreadLocalRandom.current().nextInt(zoomojis.length)];
  }
}
