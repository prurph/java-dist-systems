import java.util.concurrent.ThreadLocalRandom;

public class PscottUtils {
  public static String zoomoji() {
    String[] zoomojis = {"🦁", "🐯", "🐵", "🐨", "🐻", "🦥", "🦜"};
    return zoomojis[ThreadLocalRandom.current().nextInt(zoomojis.length)];
  }
}

