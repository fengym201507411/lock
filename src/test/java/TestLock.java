import com.fym.LockService;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by fengyiming on 2019/1/1.
 */
public class TestLock {

    public static void main(String[] args) throws IOException {
        ZooKeeper zooKeeper = new ZooKeeper("127.0.0.1:2181", 10000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {

            }
        });
        ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 10, 0, TimeUnit.SECONDS, new
            LinkedBlockingQueue<Runnable>());
        for (int i = 0; i < 1000; i++) {
            String name = new StringBuffer("ThreadName[").append(i).append("]").toString();
            executor.execute(() -> {
                try {
                    LockService.lock(zooKeeper, "firstLock",() -> {
                        System.out.println(name);
                    });
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
    }
}
