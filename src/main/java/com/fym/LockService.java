package com.fym;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created by fengyiming on 2018/12/24.
 * 基于zookeeper的代码
 */
public class LockService {

    /**
     * 分布式锁创建key的根路径
     */
    private final static String PRE_ROOT_PATH = "/zkLockRoot";

    /**
     * /字符
     */
    private final static String PATH = "/";

    private static Logger logger = LoggerFactory.getLogger(LockService.class);

    /**
     * 完成所有的加锁执行，释放锁的过程
     * 注意临时节点的有效时间要比执行lock方法长很多,不建议设置太小，否则达不到锁定的效果
     *
     * @param zooKeeper
     * @param key
     * @param lockFunction
     */
    public static void lock(ZooKeeper zooKeeper, String key, LockFunction lockFunction) throws Exception {
        try {
            Stat rootStat = zooKeeper.exists(PRE_ROOT_PATH, false);
            if (rootStat == null) {
                try {
                    zooKeeper.create(PRE_ROOT_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    logger.info("root path:{} create success", PRE_ROOT_PATH);
                } catch (KeeperException k) {
                    logger.error("root path exists", k.getMessage());
                }
            }
            String path = new StringBuffer(PRE_ROOT_PATH).append(PATH).append(key).toString();
            Stat stat = zooKeeper.exists(path, false);
            if (stat == null) {
                try {
                    zooKeeper.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    logger.info("key path:{} create success", path);
                } catch (KeeperException k) {
                    logger.error("key path:{} exists", path);
                }
            }
            String lockNodePre = new StringBuffer(PRE_ROOT_PATH).append(PATH).append(key).append(PATH).toString();
            String lockNode = zooKeeper.create(lockNodePre, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode
                .EPHEMERAL_SEQUENTIAL);
            String lockNodeName = lockNode.substring(lockNodePre.length());
            // 拿到所有子节点，这里不需要去监听每个临时有序节点
            List<String> subNodes = zooKeeper.getChildren(path, false);
            //排序
            subNodes.sort(String::compareTo);
            //当前节点在所有节点里的顺序
            int index = subNodes.indexOf(lockNodeName);
            //当前最小的节点
            String minNodeName = subNodes.get(0);
            if (!lockNodeName.equals(minNodeName)) {
                //去监听比自己小一点的节点
                String min1NodeName = subNodes.get(index - 1);
                String min1NodePath = new StringBuffer(PRE_ROOT_PATH).append(PATH).append(key).append(PATH)
                    .append(min1NodeName).toString();
                CountDownLatch countDownLatch = new CountDownLatch(1);
                Stat min1Stat = zooKeeper.exists(min1NodePath, new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        if (event.getType() == Event.EventType.NodeDeleted) {
                            countDownLatch.countDown();
                        }
                    }
                });
                if (min1Stat != null) {
                    countDownLatch.await();
                }
            }
            lockFunction.lock();
            //释放锁
            zooKeeper.delete(lockNode, -1);
            try {
                zooKeeper.delete(path, -1);
            } catch (KeeperException k) {
                logger.error("try delete path failed", k);
            }
        } catch (Exception e) {
            logger.info("lock error" + e.getMessage());
            throw new Exception(e.getMessage());
        }
    }
}
