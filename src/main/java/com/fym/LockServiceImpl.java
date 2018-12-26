package com.fym;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.stream.Collector;

/**
 * Created by fengyiming on 2018/12/24.
 */
public class LockServiceImpl {

    private static volatile ZooKeeper zooKeeper;

    private static volatile LockWatcher lockWatcher;

    /**
     * zk连接超时时间/s
     */
    private final static int SESSION_TIMEOUT = 30000;

    /**
     * 分布式锁创建key的根路径
     */
    private final static String PRE_ROOT_PATH = "/zkLockRoot";

    /**
     * 分布式锁创建的节点的名称前缀
     */
    private final static String PRE_NODE = "lock";

    /**
     * /字符
     */
    private final static String PATH = "/";

    static {
        lockWatcher = new LockWatcher();
        try {
            // 连接zookeeper
            zooKeeper = new ZooKeeper("127.0.0.1:2181", SESSION_TIMEOUT, lockWatcher);
            Stat stat = zooKeeper.exists(PRE_ROOT_PATH, false);
            if(stat == null){
                System.out.println("root path is null,create......");
                zooKeeper.create(PRE_ROOT_PATH,new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            System.out.println("root path ok");
        } catch (Exception e) {
            System.out.println("加载zk信息异常" + e.getMessage());
        }
    }


    public static void lock(String key) {
        try {
            System.out.println("begin lock");
            String path = new StringBuffer(PRE_ROOT_PATH).append(PATH).append(key).toString();
            Stat stat = zooKeeper.exists(path, false);
            if(stat == null){
                System.out.println("key  path is null,create......");
                zooKeeper.create(path,new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            String lockNodePre = new StringBuffer(PRE_ROOT_PATH).append(PATH).append(key).append(PATH).toString();
            String lockNode = zooKeeper.create(lockNodePre, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode
                .EPHEMERAL_SEQUENTIAL);
            String lockNodeName = lockNode.substring(lockNodePre.length());
            System.out.println("创建有序临时节点:" + lockNode+",节点名称：" + lockNodeName);
            // 取所有子节点
            List<String> subNodes = zooKeeper.getChildren(path, false);
            System.out.println("当前竞争资源下的节点数：" + subNodes.size());
            subNodes.sort(String::compareTo);
            System.out.println("first:" + subNodes.get(0) + ",last: "+ subNodes.get(subNodes.size() - 1));
            if(lockNodeName.equals(subNodes.get(0))){
                System.out.println("拿到了lock，do -----");
                zooKeeper.delete(lockNode,-1);
                System.out.println("执行完毕，解锁------");
            }
        }catch (Exception e){
            System.out.println("loc0 error" + e.getMessage());
        }
    }

    public static void main(String[] args) {
        LockServiceImpl.lock("firstLock");
    }
}
