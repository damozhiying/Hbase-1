package base;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * <p/>
 * <li>Description:</li>
 * <li>@author: lee </li>
 * <li>Date: 2017/10/15 </li>
 * <li>@version: 1.0.0 </li>
 */
public class ZookeeperTest {

    String connect = "hadoop-001:2181,hadoop-002:2181,hadoop-003:2181";

    ZooKeeper zk = null;

    /**
     * 连接到zookeeper
     */
    @Before
    public void getConn () throws IOException {

        zk = new ZooKeeper( connect, 6000, new Watcher() {

            @Override
            public void process ( WatchedEvent watchedEvent ) {

                System.out.println( "监控所有被触发的事件，EVENT:" + watchedEvent.getType() );
            }
        } );
        System.out.println( "=========连接成功==========" );
    }

    /**
     * 创建节点
     */
    @Test
    public void createNode () throws IOException, KeeperException, InterruptedException {

        getConn();

        ACL acl = new ACL( ZooDefs.Perms.ALL, ZooDefs.Ids.ANYONE_ID_UNSAFE );
        List< ACL > aclList = new ArrayList< ACL >();
        aclList.add( acl );
        zk.create( "/root", "node1".getBytes(), aclList, CreateMode.PERSISTENT );
        System.out.println( "============创建节点成功===========" );
    }
}
