import GHS.Node;
import cache.MessageCacheQueue;
import cache.NeighbourCache;
import cache.NodeCache;

import java.io.*;
import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.*;


import carrier.NodeCarrier;
import com.fasterxml.jackson.annotation.JsonIgnore;
import loader.LoadGraphToPlatform;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import repository.EdgeRepository;
import repository.NodeRepository;
import utils.JdbcDataSource;
import utils.RedisDataSource;

public class Main {

    public static long beginning_time_millis;

    public static void main(String[] args)
            throws IOException, SQLException, ExecutionException, InterruptedException {
        System.out.println("You want to load new graph??[y/n]");
        Scanner scanner = new Scanner(System.in);
        String s = scanner.nextLine();
        int graphSize = 100;
        if (s.toUpperCase().equals("Y")) {
            LoadGraphToPlatform loadGraphToPlatform = new LoadGraphToPlatform();
            System.out.println("Please enter file name:");
            String filename = scanner.nextLine();
            System.out.println("Is edges are double showing?? (a->b, b->a)[y/n]");
            s = scanner.nextLine();
            boolean oneSide = false;
            if (s.toUpperCase().equals("Y")) {
                oneSide = true;
            }
            try (Jedis jedis = RedisDataSource.getResource()) {
                jedis.flushAll();
            }
            graphSize = loadGraphToPlatform.initialLoadFromTextFile("/" + filename, oneSide);
        } else {
            String graphSizeQuery = "Select Count(*) from tomato.\"Node\"";
            try (Connection connection = JdbcDataSource.getConnection(); PreparedStatement ps = connection.prepareStatement(graphSizeQuery)) {
                ResultSet resultSet = ps.executeQuery();
                resultSet.next();
                graphSize = resultSet.getInt(1);
            }
        }
        EdgeRepository edgeRepository = new EdgeRepository();
        NodeRepository nodeRepository = new NodeRepository(edgeRepository);
        NodeCache nodeCache = new NodeCache(nodeRepository);
        LinkedBlockingQueue<String> nodeQueue = new LinkedBlockingQueue<>();
        MessageCacheQueue messageCacheQueue = new MessageCacheQueue();
        int first = 0;
        int step = 10000;
        int numThreads = 20;
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        List<NodeCarrier> nodeCarriers = new LinkedList<>();
        for (int i = 0; i < numThreads; i++) {
            NodeCarrier nodeCarrier = new NodeCarrier(nodeQueue, nodeCache, step, messageCacheQueue);
            nodeCarriers.add(nodeCarrier);
            //executorService.submit(nodeCarrier);
        }
        beginning_time_millis = System.currentTimeMillis();
        List<Future<List<Node>>> slavesResult = new LinkedList<>();
        List<Node> results = new LinkedList<>();
        boolean isOdd = true;
        String deleteNeighbour = "Truncate tomato.\"Neighbour\"";
        String deleteNeighbour2 = "Truncate tomato.\"Neighbour2\"";
        String deleteNode = "Truncate tomato.\"Node\"";
        String deleteNode2 = "Truncate tomato.\"Node2\"";
        String restartSeq = "ALTER SEQUENCE tomato.\"Node_iid_seq\" RESTART WITH 1;\n";
        String restartSeq2 = "ALTER SEQUENCE tomato.\"Node2_iid_seq\" RESTART WITH 1;\n";
        while (true) {
            try (Jedis jedis = RedisDataSource.getResource()) {
                  if (nodeQueue.isEmpty()) {
                    if (jedis.keys("finishNode%%*").size() == graphSize) {
                        System.out.println("Finish");
                        break;
                    }
                    nodeRepository.loadTrivial(first, first + step, isOdd);
                    nodeQueue.addAll(jedis.keys("node%%*"));
                    if(first > graphSize){
                        try(Connection connection = JdbcDataSource.getConnection()){
                            PreparedStatement ps1 = connection.prepareStatement(isOdd ? deleteNeighbour : deleteNeighbour2);
                            ps1.execute();
                            PreparedStatement ps2 = connection.prepareStatement(isOdd ? deleteNode : deleteNode2);
                            ps2.execute();
                            PreparedStatement ps3 = connection.prepareStatement(isOdd ? restartSeq : restartSeq2);
                            ps3.execute();
                            ps1.close();
                            ps2.close();
                            ps3.close();
                        }
                        isOdd = !isOdd;
                        first = -1 * step;
                    }
                    first += step;
                }else{
                    for (NodeCarrier nodeCarrier: nodeCarriers) {
                        slavesResult.add(executorService.submit(nodeCarrier));
                    }
                    for (Future<List<Node>> future: slavesResult) {
                        results.addAll(future.get());
                    }
                    slavesResult.clear();
                    nodeRepository.updateBatch(results, isOdd);
                    results.clear();
                }
            }
        }
        executorService.shutdown();
        System.out.println("exec Time until now : " + (System.currentTimeMillis() - beginning_time_millis));
    }
}
