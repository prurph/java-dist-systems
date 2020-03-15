import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class LeaderElection implements Watcher {
  private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
  private static final int SESSION_TIMEOUT = 3000;
  private static final String ELECTION_NAMESPACE = "/election";

  private String zoomoji;
  private ZooKeeper zooKeeper;
  private String currentZnodeName;

  public LeaderElection() {
    this.zoomoji = PscottUtils.zoomoji();
  }

  public static void main(String[] args) throws InterruptedException, IOException, KeeperException {
    LeaderElection leaderElection = new LeaderElection();

    leaderElection.connectToZookeeper();
    leaderElection.volunteerAsLeader();
    leaderElection.reelectLeader();
    leaderElection.run();
    leaderElection.close();

    System.out.println("👋 Disconnected from ZooKeeper");
  }

  public void volunteerAsLeader() throws InterruptedException, KeeperException {
    String znodePrefix = ELECTION_NAMESPACE + "/c_";
    String znodeFullPath = zooKeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE,
        CreateMode.EPHEMERAL_SEQUENTIAL);

    System.out.println(zoomoji + " assigned znode name: " + znodeFullPath);
    this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
  }

  public void reelectLeader() throws KeeperException, InterruptedException {
    Stat predecessorStat = null;
    String predecessorZnodeName = "";

    // Must wrap in while loop to avoid race condition where the node we want to watch dies in
    // between the .getChildren and .exists call (in that case .exists returns null)
    while (predecessorStat == null) {
      List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);

      Collections.sort(children);
      String smallestChild = children.get(0);

      if (smallestChild.equals(currentZnodeName)) {
        System.out.println(zoomoji + " I am the leader");
        return;
      } else {
        System.out.println(zoomoji + " I am not the leader");
        int predecessorIndex = Collections.binarySearch(children, currentZnodeName) - 1;
        predecessorZnodeName = children.get(predecessorIndex);
        predecessorStat = zooKeeper.exists(ELECTION_NAMESPACE + "/" + predecessorZnodeName,
            this);
      }

      System.out.println(zoomoji + " I am watching " + predecessorZnodeName);
    }
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
          System.out.println(zoomoji + " Connected to ZooKeeper");
        } else {
          synchronized (zooKeeper) {
            System.out.println(zoomoji + " Received disconnected from ZooKeeper event");
            zooKeeper.notifyAll();
          }
        }
        break;
      case NodeDeleted:
        try {
          reelectLeader();
        } catch (KeeperException | InterruptedException ignored) {
        }
        break;
    }
  }
}
