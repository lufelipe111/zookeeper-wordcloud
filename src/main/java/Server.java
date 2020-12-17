import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;


public class Server {
    static String address = "localhost";
    static ZooKeeperHelper.Queue queue = new ZooKeeperHelper.Queue(address, "/Queue");
    // Creating the hashtable that will store the words quantities
    static Hashtable<String, Integer> wordsCount = new Hashtable<String, Integer>();
    static ZooKeeper zk = ZooKeeperHelper.zk;

    public static byte[] hashTableToByteArray (Hashtable<String, Integer> ht) throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream(50000);
        ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(byteStream));
        oos.writeObject(ht);
        oos.close();

        byte[] byteArray = byteStream.toByteArray();
        return byteArray;
    }

    public static void writeTableToCSV(Hashtable<String, Integer> ht) throws IOException {
        SimpleDateFormat dateFormatter = new SimpleDateFormat("ddMMyyyyHHmmss");
        Date date = new Date();
        FileWriter csvWriter = new FileWriter("word-cloud-" + dateFormatter.format(date) + ".csv");

        // Create header
        csvWriter.append("Palavra");
        csvWriter.append(",");
        csvWriter.append("Quantidade");
        csvWriter.append("\n");

        // get data
        for(String s : ht.keySet()) {
            List<String> row = Arrays.asList(s, ht.get(s).toString());
            csvWriter.append(String.join(",", row));
            csvWriter.append("\n");
        }

        // Write data to file
        csvWriter.flush();
        csvWriter.close();
    }

    public static void main(String[] args) throws KeeperException, InterruptedException, IOException {
        // znode to store voting data
        String dataPath = "/tableData";
        zk.create(dataPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        // znode to keep session active
        String keepSession = "/session";
        zk.create(keepSession, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        int v = 0;

        // Watcher to run procedure if session has ended
        new Thread(new Runnable() {
            @Override
            public void run() {
                while(true) {
                    try {
                        if (zk.exists(keepSession, false) == null) {
                            // write data to file
                            writeTableToCSV(wordsCount);
                            // re-create keepSession znode to start a new session
                            zk.create(keepSession, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                            wordsCount = new Hashtable<String, Integer>();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();

        while(true) {
            // Getting the words sent by the clients
            String word = queue.consume();

            // Verifying if the word is already in the table
            Integer q = wordsCount.get(word);
            wordsCount.put(word, (q == null)? 1 : q+1);

            // Updating data in zookeeper znode
            zk.setData(dataPath, hashTableToByteArray(wordsCount), v);
            v++;

            // print voting status
            for(String w : wordsCount.keySet()) {
                System.out.println(w + " " + wordsCount.get(w));
            }
            System.out.println("======================\n");
        }
    }
}
