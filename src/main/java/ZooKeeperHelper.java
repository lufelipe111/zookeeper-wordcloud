import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class ZooKeeperHelper implements Watcher {

    static ZooKeeper zk = null;
    static Integer mutex;

    String root;

    ZooKeeperHelper(String address) {
        if (zk == null) {
            try {
                System.out.println("Starting ZK:");
                zk = new ZooKeeper(address, 3000, this);
                mutex = new Integer(-1);
                System.out.println("Finished starting ZK: " + zk);
            } catch (IOException e) {
                System.out.println(e.toString());
                zk = null;
            }
        }
        //else mutex = new Integer(-1);
    }

    synchronized public void process(WatchedEvent event) {
        synchronized (mutex) {
            //System.out.println("Process: " + event.getType());
            mutex.notify();
        }
    }

    /**
     * Barrier
     */
    public static class Barrier extends ZooKeeperHelper {
        int size;
        String name;

        /**
         * Barrier constructor
         *
         * @param address
         * @param root
         * @param size
         */
        Barrier(String address, String root, int size) {
            super(address);
            this.root = root;
            this.size = size;

            // Create barrier node
            if (zk != null) {
                try {
                    Stat s = zk.exists(root, false);
                    if (s == null) {
                        zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                } catch (KeeperException e) {
//                    System.out
//                            .println("Keeper exception when instantiating queue: "
//                                    + e.toString());
                } catch (InterruptedException e) {
//                    System.out.println("Interrupted exception");
                }
            }

            // My node name
            try {
                name = new String(InetAddress.getLocalHost().getCanonicalHostName().toString());
            } catch (UnknownHostException e) {
//                System.out.println(e.toString());
            }

        }

        /**
         * Join barrier
         *
         * @return
         * @throws KeeperException
         * @throws InterruptedException
         */

        boolean enter() throws KeeperException, InterruptedException {
            zk.create(root + "/" + name, new byte[0], Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL_SEQUENTIAL);
            while (true) {
                synchronized (mutex) {
                    List<String> list = zk.getChildren(root, true);
                    System.out.println("Esperando outros jogadores");
                    if (list.size() < size) {
                        mutex.wait();
                    } else {
                        return true;
                    }
                }
            }
        }

        /**
         * Wait until all reach barrier
         *
         * @return
         * @throws KeeperException
         * @throws InterruptedException
         */

        boolean leave() throws KeeperException, InterruptedException {
            zk.delete(root + "/" + name, 0);
            while (true) {
                synchronized (mutex) {
                    List<String> list = zk.getChildren(root, true);
                    if (list.size() > 0) {
                        mutex.wait();
                    } else {
                        return true;
                    }
                }
            }
        }
    }

    /**
     * Producer-Consumer queue
     */
    public static class Queue extends ZooKeeperHelper {

        /**
         * Constructor of producer-consumer queue
         *
         * @param address
         * @param name
         */
        Queue(String address, String name) {
            super(address);
            this.root = name;
            // Create ZK node name
            if (zk != null) {
                try {
                    Stat s = zk.exists(root, false);
                    if (s == null) {
                        zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                } catch (KeeperException e) {
//                    System.out.println("Keeper exception when instantiating queue: " + e.toString());
                } catch (InterruptedException e) {
//                    System.out.println("Interrupted exception");
                }
            }
        }

        /**
         * Add element to the queue.
         *
         * @param msg
         * @return
         */

        boolean produce(String msg) throws KeeperException, InterruptedException {
            ByteBuffer b = ByteBuffer.allocate(4);
            byte[] value;
            value = msg.getBytes();
            // Add child with value i
            // b.putInt(i);
            // value = b.array();
//            System.out.println("creating" + b.toString());
            zk.create(root + "/element", value, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

            return true;
        }


        /**
         * Remove first element from the queue.
         *
         * @return
         * @throws KeeperException
         * @throws InterruptedException
         */
        String consume() throws KeeperException, InterruptedException {
            String retvalue = "";
            Stat stat = null;

            // Get the first element available
            while (true) {
                synchronized (mutex) {
                    List<String> list = zk.getChildren(root, true);
                    if (list.size() == 0) {
//                        System.out.println("Going to wait");
                        mutex.wait();
                    } else {
                        Integer min = new Integer(list.get(0).substring(7));
//                        System.out.println("List: " + list.toString());
                        String minString = list.get(0);
                        for (String s : list) {
                            Integer tempValue = new Integer(s.substring(7));
                            //System.out.println("Temp value: " + tempValue);
                            if (tempValue < min) {
                                min = tempValue;
                                minString = s;
                            }
                        }
//                        System.out.println("Temporary value: " + root + "/" + minString);
                        byte[] b = zk.getData(root + "/" + minString, false, stat);
                        // System.out.println("b: " + Arrays.toString(b));
                        zk.delete(root + "/" + minString, 0);
                        // ByteBuffer buffer = ByteBuffer.wrap(b);
                        retvalue = new String(b);
                        return retvalue;
                    }
                }
            }
        }
    }

    /**
     * Lock znode
     */
    public static class Lock extends ZooKeeperHelper {
        long wait;
        String pathName;

        /**
         * Constructor of lock
         *
         * @param address
         * @param name    Name of the lock node
         */
        Lock(String address, String name, long waitTime) {
            super(address);
            this.root = name;
            this.wait = waitTime;
            // Create ZK node name
            if (zk != null) {
                try {
                    Stat s = zk.exists(root, false);
                    if (s == null) {
                        zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                } catch (KeeperException e) {
//                    System.out.println("Keeper exception when instantiating queue: " + e.toString());
                } catch (InterruptedException e) {
//                    System.out.println("Interrupted exception");
                }
            }
        }

        boolean lock() throws KeeperException, InterruptedException {
            //Step 1
            pathName = zk.create(root + "/lock-", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
//            System.out.println("My path name is: " + pathName);
            //Steps 2 to 5
            return testMin();
        }

        boolean testMin() throws KeeperException, InterruptedException {
            while (true) {
                Integer suffix = new Integer(pathName.substring(12));
                //Step 2
                List<String> list = zk.getChildren(root, false);
                Integer min = new Integer(list.get(0).substring(5));
//                System.out.println("List: " + list.toString());
                String minString = list.get(0);
                for (String s : list) {
                    Integer tempValue = new Integer(s.substring(5));
                    //System.out.println("Temp value: " + tempValue);
                    if (tempValue < min) {
                        min = tempValue;
                        minString = s;
                    }
                }
//                System.out.println("Suffix: " + suffix + ", min: " + min);
                //Step 3
                if (suffix.equals(min)) {
//                    System.out.println("Lock acquired for " + minString + "!");
                    return true;
                }
                //Step 4
                //Wait for the removal of the next lowest sequence number
                Integer max = min;
                String maxString = minString;
                for (String s : list) {
                    Integer tempValue = new Integer(s.substring(5));
                    //System.out.println("Temp value: " + tempValue);
                    if (tempValue > max && tempValue < suffix) {
                        max = tempValue;
                        maxString = s;
                    }
                }
                //Exists with watch
                Stat s = zk.exists(root + "/" + maxString, this);
//                System.out.println("Watching " + root + "/" + maxString);
                //Step 5
                if (s != null) {
                    //Wait for notification
                    break;
                }
            }
//            System.out.println(pathName + " is waiting for a notification!");
            return false;
        }

        synchronized public void process(WatchedEvent event) {
            synchronized (mutex) {
                String path = event.getPath();
                if (event.getType() == Event.EventType.NodeDeleted) {
//                    System.out.println("Notification from " + path);
                    try {
                        if (testMin()) { //Step 5 (cont.) -> go to step 2 to check
                            this.compute();
                        } else {
//                            System.out.println("Not lowest sequence number! Waiting for a new notification.");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        void compute() {
//            System.out.println("Lock acquired!");
            try {
                new Thread().sleep(wait);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //Exits, which releases the ephemeral node (Unlock operation)
//            System.out.println("Lock released!");
        }
    }

    public static class Leader extends ZooKeeperHelper {
        String leader;
        String id; //Id of the leader
        String pathName;

        /**
         * Constructor of Leader
         *
         * @param address
         * @param name Name of the election node
         * @param leader Name of the leader node
         *
         */
        Leader(String address, String name, String leader, int id) {
            super(address);
            this.root = name;
            this.leader = leader;
            this.id = new Integer(id).toString();
            // Create ZK node name
            if (zk != null) {
                try {
                    //Create election znode
                    Stat s1 = zk.exists(root, false);
                    if (s1 == null) {
                        zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                    //Checking for a leader
                    Stat s2 = zk.exists(leader, false);
                    if (s2 != null) {
                        byte[] idLeader = zk.getData(leader, false, s2);
//                        System.out.println("Current leader with id: "+new String(idLeader));
                    }

                } catch (KeeperException e) {
//                    System.out.println("Keeper exception when instantiating queue: " + e.toString());
                } catch (InterruptedException e) {
//                    System.out.println("Interrupted exception");
                }
            }
        }

        boolean elect() throws KeeperException, InterruptedException{
            this.pathName = zk.create(root + "/n-", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
//            System.out.println("My path name is: "+pathName+" and my id is: "+id+"!");
            return check();
        }

        boolean check() throws KeeperException, InterruptedException{
            Integer suffix = new Integer(pathName.substring(12));
            while (true) {
                List<String> list = zk.getChildren(root, false);
                Integer min = new Integer(list.get(0).substring(5));
//                System.out.println("List: "+list.toString());
                String minString = list.get(0);
                for(String s : list){
                    Integer tempValue = new Integer(s.substring(5));
                    //System.out.println("Temp value: " + tempValue);
                    if(tempValue < min)  {
                        min = tempValue;
                        minString = s;
                    }
                }
//                System.out.println("Suffix: "+suffix+", min: "+min);
                if (suffix.equals(min)) {
                    this.leader();
                    return true;
                }
                Integer max = min;
                String maxString = minString;
                for(String s : list){
                    Integer tempValue = new Integer(s.substring(5));
                    //System.out.println("Temp value: " + tempValue);
                    if(tempValue > max && tempValue < suffix)  {
                        max = tempValue;
                        maxString = s;
                    }
                }
                //Exists with watch
                Stat s = zk.exists(root+"/"+maxString, this);
//                System.out.println("Watching "+root+"/"+maxString);
                //Step 5
                if (s != null) {
                    //Wait for notification
                    break;
                }
            }
//            System.out.println(pathName+" is waiting for a notification!");
            return false;
        }

        synchronized public void process(WatchedEvent event) {
            synchronized (mutex) {
                if (event.getType() == Event.EventType.NodeDeleted) {
                    try {
                        boolean success = check();
                        if (success) {
//                            System.out.println("Become leader");
                        }
                    } catch (Exception e) {e.printStackTrace();}
                }
            }
        }

        void leader() throws KeeperException, InterruptedException {
//            System.out.println("Become a leader: "+id+"!");
            //Create leader znode
            Stat s2 = zk.exists(leader, false);
            if (s2 == null) {
                zk.create(leader, id.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } else {
                zk.setData(leader, id.getBytes(), 0);
            }
        }

        void compute() {
            // System.out.println("I will die after 10 seconds!");
            try {
                new Thread().sleep(10000);
                // System.out.println("Process "+id+" died!");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.exit(0);
        }
    }
}
