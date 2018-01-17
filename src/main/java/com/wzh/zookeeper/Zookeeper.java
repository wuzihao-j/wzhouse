package com.wzh.zookeeper;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;

public class Zookeeper {

    private static ZooKeeper zk;

    public static void doOne(String root, String nodeName, String port) throws Exception {

        zk = new ZooKeeper("120.78.205.73:" + port, 60000, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                if(watchedEvent.getPath() != null && watchedEvent.getPath().equals("/queue/profit")
                        && watchedEvent.getType() == Event.EventType.NodeCreated){
                    System.out.println("开始计算利润");
                }
            }
        });

        createNode(root, nodeName);
    }

    public static void createNode(String root, String nodeName) throws KeeperException, InterruptedException {
        if(zk.exists("/" + root, true) == null){
            zk.create("/" + root, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        zk.create("/" + root + "/" + nodeName, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        List<String> children = zk.getChildren("/" + root, false);
        if(children.contains("purchase") && children.contains("sell")){
            zk.create("/queue/profit", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }



}
