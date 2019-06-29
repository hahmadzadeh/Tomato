import GHS.Edge;
import GHS.Node;
import cache.MessageCacheQueue;
import cache.NeighbourCache;
import cache.NodeCache;

import java.io.*;
import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import loader.LoadGraphToPlatform;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import repository.EdgeRepository;
import repository.NodeRepository;
import utils.JdbcDataSource;

public class Main {

    public static void main(String[] args)
            throws IOException, SQLException, ExecutionException, InterruptedException {
        System.out.println("You want to load new graph??[y/n]");
        Scanner scanner = new Scanner(System.in);
        String s = scanner.nextLine();
        JedisPool pool = MessageCacheQueue.jedisPool;
        int graphSize = 100;
        if (s.toUpperCase().equals("Y")) {
            LoadGraphToPlatform loadGraphToPlatform = new LoadGraphToPlatform();
            System.out.println("Please enter file name:");
            String filename = scanner.nextLine();
            try (Jedis jedis = pool.getResource()) {
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
        LinkedList<String> nodeQueue = new LinkedList<>();
        MessageCacheQueue messageCacheQueue = new MessageCacheQueue();
        int first = 0;
        int step = 100;
        int numThreads = 25;
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        List<Future<Node>> slavesResult = new LinkedList<>();
        while (true) {
            try (Jedis jedis = pool.getResource()) {
                if (nodeQueue.isEmpty()) {
                    nodeCache.flush("node%%", Node.class, nodeRepository, false);
                    nodeRepository.loadTrivial(first, first + step);
                    nodeQueue.addAll(jedis.keys("node%%*"));
                    first += step;
                    first = first > graphSize ? 0 : first;
                }
                for (int i = 0; i < numThreads; i++) {
                    if (nodeQueue.isEmpty()) {
                        break;
                    }
                    String key = nodeQueue.poll();
                    Node node = nodeCache.getNode(key);
                    node.msgQueue = messageCacheQueue;
                    slavesResult.add(executorService.submit(node));
                }
                for (Future<Node> future : slavesResult) {
                    nodeCache.addNode(future.get(), false);
                }
                slavesResult.clear();
            }
        }
    }
}
