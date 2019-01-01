package com.fym;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by fengyiming on 2018/12/24.
 * 基于zookeeper的代码
 */
public class LockServiceImpl {

    private static volatile ZooKeeper zooKeeper;

    /**
     * zk连接超时时间/s
     */
    private final static int SESSION_TIMEOUT = 10000;

    /**
     * 分布式锁创建key的根路径
     */
    private final static String PRE_ROOT_PATH = "/zkLockRoot";

    /**
     * /字符
     */
    private final static String PATH = "/";

    static {
        try {
            // 连接zookeeper
            zooKeeper = new ZooKeeper("127.0.0.1:2181", SESSION_TIMEOUT, new Watcher() {
                @Override
                public void process(WatchedEvent event) {

                }
            });
            Stat stat = zooKeeper.exists(PRE_ROOT_PATH, false);
            if (stat == null) {
                System.out.println("root path is null,create......");
                zooKeeper.create(PRE_ROOT_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            System.out.println("root path ok");
        } catch (Exception e) {
            System.out.println("加载zk信息异常" + e.getMessage());
        }
    }


    public static void lock(String threadName, String key) {
        try {
            key = key + LocalDate.now().toString();
            System.out.println(threadName + "begin lock");
            String path = new StringBuffer(PRE_ROOT_PATH).append(PATH).append(key).toString();
            Stat stat = zooKeeper.exists(path, false);
            if (stat == null) {
                System.out.println(threadName + "key  path is null,create......");
                try {
                    zooKeeper.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (KeeperException k) {
                    System.out.println(threadName + "目录" + path + "已存在，无需重复创建");
                } catch (Exception e) {
                    System.out.println(threadName + "create key path error,error message:" + e.getMessage());
                }
            }
            String lockNodePre = new StringBuffer(PRE_ROOT_PATH).append(PATH).append(key).append(PATH).toString();
            String lockNode = zooKeeper.create(lockNodePre, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode
                .EPHEMERAL_SEQUENTIAL);
            String lockNodeName = lockNode.substring(lockNodePre.length());
            System.out.println(threadName + "创建有序临时节点:" + lockNode + ",节点名称：" + lockNodeName);
            // 取所有子节点
            List<String> subNodes = zooKeeper.getChildren(path, false);
            System.out.println(threadName + "当前竞争资源下的节点数：" + subNodes.size());
            //排序
            subNodes.sort(String::compareTo);
            System.out.println(threadName + "first:" + subNodes.get(0) + ",last: " + subNodes.get(subNodes.size() - 1));
            int index = subNodes.indexOf(lockNodeName);
            String minNodeName = subNodes.get(0);
            if (!lockNodeName.equals(minNodeName)) {
                String min1NodeName = subNodes.get(index - 1);
                String min1NodePath = new StringBuffer(PRE_ROOT_PATH).append(PATH).append(key).append(PATH)
                    .append(min1NodeName).toString();
                CountDownLatch countDownLatch = new CountDownLatch(1);
                System.out.println(threadName + "当前节点" + lockNodeName + "准备监听节点：" + min1NodeName);
                Stat min1Stat = zooKeeper.exists(min1NodePath, new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        System.out.println(threadName + "节点" + event.getPath() + "，事件 : " + event.getType());
                        if (event.getType() == Event.EventType.NodeDeleted) {
                            System.out.println(threadName + "节点删除");
                            countDownLatch.countDown();
                        }
                    }
                });
                if (min1Stat == null) {
                    System.out.println(threadName + "节点不存在，无需等待，当前节点：" + lockNodeName + "，前一节点：" + min1NodeName);
                } else {
                    System.out.println(threadName + "------wait-------");
                    countDownLatch.await();
                }
            }
            System.out.println(threadName + "拿到了lock" + lockNodeName + "，do -----");
            zooKeeper.delete(lockNode, -1);
            System.out.println(threadName + "执行完毕，解锁" + lockNodeName + "------");
            String lastNodeName = subNodes.get(subNodes.size() - 1);
            if (lockNodeName.equals(lastNodeName)) {
                try {
                    zooKeeper.delete(path, -1);
                    System.out.println(threadName + "尝试删除该key目录成功，path" + path);
                } catch (KeeperException k) {
                    System.out.println(threadName + "尝试删除该key目录，失败：" + k.getMessage());
                } catch (Exception e) {
                    System.out.println(threadName + "尝试删除该key目录，失败：" + e.getMessage());
                }
            }
        } catch (Exception e) {
            System.out.println(threadName + "lock error" + e.getMessage());
        }
    }

    public static void main(String[] args) throws InterruptedException {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 10, 0, TimeUnit.SECONDS, new
            LinkedBlockingQueue<Runnable>());
        for (int i = 0; i < 1000; i++) {
            String name = new StringBuffer("ThreadName[").append(i).append("]").toString();
            executor.execute(() -> {
                LockServiceImpl.lock(name, "firstLock");
            });
        }
    }
}
