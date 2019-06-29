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
            try (Jedis jedis = RedisDataSource.getResource()) {
                jedis.flushAll();
            }
            graphSize = loadGraphToPlatform.initialLoadFromTextFile("/" + filename);
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
        int step = 100;
        int numThreads = 8;
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        List<NodeCarrier> nodeCarriers = new LinkedList<>();
        for (int i = 0; i < numThreads; i++) {
            NodeCarrier nodeCarrier = new NodeCarrier(nodeQueue, nodeCache, step, messageCacheQueue);
            nodeCarriers.add(nodeCarrier);
            executorService.submit(nodeCarrier);
        }

        beginning_time_millis = System.currentTimeMillis();
        List<Future<List<Node>>> slavesResult = new LinkedList<>();
        List<Node> results = new LinkedList<>();
        while (true) {
            try (Jedis jedis = RedisDataSource.getResource()) {
                if (nodeQueue.isEmpty()) {
                    if (jedis.keys("finishNode%%*").size() == graphSize) {
                        System.out.println("Finish");
                        break;
                    }
                    nodeRepository.loadTrivial(first, first + step);
                    nodeQueue.addAll(jedis.keys("node%%*"));
                    first += step;
                    first = first > graphSize ? 0 : first;
                }else{
                    for (NodeCarrier nodeCarrier: nodeCarriers) {
                        slavesResult.add(executorService.submit(nodeCarrier));
                    }
                    for (Future<List<Node>> future: slavesResult) {
                        results.addAll(future.get());
                    }
                    slavesResult.clear();
                    nodeRepository.updateBatch(results);
                    results.clear();
                }
            }
        }
        executorService.shutdown();
        System.out.println("exec Time until now : " + (System.currentTimeMillis() - beginning_time_millis));
    }
}
