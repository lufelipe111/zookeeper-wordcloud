import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.Hashtable;
import java.util.Random;
import java.util.Scanner;

public class Client {
    static String address = "localhost";
    static Random random = new Random();
    static int r = random.nextInt(1000000);
    static ZooKeeperHelper.Queue queue = new ZooKeeperHelper.Queue(address, "/Queue");
    static ZooKeeperHelper.Lock lock = new ZooKeeperHelper.Lock(address, "/lock", 5000);
    static ZooKeeperHelper.Leader leader = new ZooKeeperHelper.Leader(address, "/election", "/clientLeader", r);
    static ZooKeeperHelper.Barrier barrier;
    static ZooKeeper zk = ZooKeeperHelper.zk;
    static Scanner scanner = new Scanner(System.in);
    static int OPTION = -1;
    static int q;

    public static String writeNote() {
        String note = scanner.next();

        return note;
    }

    public static void defaultMenu() throws KeeperException, InterruptedException {
        System.out.println("Insira a opção: \n" +
                "(1) Inserir palavra\n" +
                "\n" +
                "=============================\n" +
                "(0) Sair");
        OPTION = scanner.nextInt();

        switch (OPTION) {
            case 1:
                String msg = writeNote();
                queue.produce(msg);
                System.out.println("Você pode executar outra tarefa em 5 segundos");
                lock.compute();
                break;
            case 0:
                System.out.println("Obrigado por utilizar!");
                System.exit(1);
                break;
            default:
                System.out.println("opção inválida tente novamente\n\n");
        }
    }

    public static Hashtable<String, Integer> bytesToHashTable(byte[] b) throws IOException, ClassNotFoundException {
        ByteArrayInputStream byteStream = new ByteArrayInputStream(b);
        ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(byteStream));
        Hashtable<String, Integer> table = (Hashtable<String, Integer>) ois.readObject();
        ois.close();

        return table;
    }

    public static void leaderMenu() throws KeeperException, InterruptedException, IOException, ClassNotFoundException {
        String dataPath = "/tableData";
        String keepSession = "/session";
        byte[] dataBytes;
        Hashtable<String, Integer> data;

        while(zk.exists(keepSession, true) != null) {
            System.out.println("Insira a opção: \n" +
                    "(1) Inserir palavra\n" +
                    "(2) Visualizar palavras\n" +
                    "(3) Encerrar grupo" +
                    "\n" +
                    "=============================\n" +
                    "(0) Sair");
            OPTION = scanner.nextInt();

            switch (OPTION) {
                case 1:
                    String msg = writeNote();
                    queue.produce(msg);
                    System.out.println("Você pode executar outra tarefa em 5 segundos");
                    lock.compute();
                    break;
                case 2:
                    try {
                        dataBytes = zk.getData(dataPath, false, null);
                        data = bytesToHashTable(dataBytes);

                        System.out.println("\n");
                        for (String s : data.keySet()) {
                            Integer q = data.get(s);
                            System.out.println(s + " " + q);
                        }
                        System.out.println("======================\n");
                    }
                    catch(Exception e) {
                        System.out.println("\n============================");
                        System.out.println("| Tabela de palavras vazia |");
                        System.out.println("============================\n");
                    }
                    break;
                case 3:
                    System.out.println("Encerrar votação");
                    zk.delete(keepSession, 0);
                    OPTION = 0;
                    break;
                case 0:
                    System.out.println("Obrigado por utilizar!");
                    System.exit(0);
                    return;
                default:
                    System.out.println("opção inválida tente novamente\n\n");
            }
        }
    }

    public static byte[] intToBytes( final int i ) {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(i);
        return bb.array();
    }

    public static int getBarrierQuantity() throws KeeperException, InterruptedException {
        byte[] b = zk.getData("/barrierQuantity", false, null);
        ByteBuffer q = ByteBuffer.wrap(b);
        return q.getInt();
    }
    public static void main(String[] args) throws KeeperException, InterruptedException, IOException, ClassNotFoundException {
        leader.elect();

        int iteration = 0;

        new Thread(new Runnable() {
            @Override
            public void run() {
                while(true) {
                    try {
                        if (zk.exists("/session", false) == null) {
                            System.out.println("a votação está encerrada\n");
                            System.exit(0);
                        }
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();

        while(OPTION != 0 & zk.exists("/session", false) != null) {
            boolean isLeader = leader.check();
            if(!isLeader) {
                if(iteration == 0) {
                    try {
                        q = getBarrierQuantity();
                        barrier = new ZooKeeperHelper.Barrier(address, "/barrier", q);
                        barrier.enter();
                    }
                    catch (Exception e) {
                        System.out.println("sem barreira");
                    }
                }
                defaultMenu();
            } else {
                System.out.println("Você é o moderador de grupo!");
                if(iteration == 0) {
                    System.out.println("Definir número mínimo de votantes: ");
                    q = scanner.nextInt();
                    barrier = new ZooKeeperHelper.Barrier(address, "/barrier", q);
                    ZooKeeperHelper.zk.create("/barrierQuantity", intToBytes(q),
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                    barrier.enter();
                }
                leaderMenu();
            }
            iteration++;
        }

    }
}
