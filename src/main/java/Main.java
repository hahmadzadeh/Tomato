import GHS.Node;
import cache.MessageCacheQueue;
import cache.NeighbourCache;
import cache.NodeCache;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import loader.LoadGraphToPlatform;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import repository.EdgeRepository;
import repository.NodeRepository;
import utils.JdbcDataSource;

public class Main {

    public static void main(String[] args)
        throws IOException, SQLException, ExecutionException, InterruptedException {
        JedisPool pool = MessageCacheQueue.jedisPool;
        try (Jedis jedis = pool.getResource()){
            jedis.flushAll();
        }
        String deleteNeighbour = "delete from tomato.\"Neighbour\" where 1=1";
        String deleteNode = "delete from tomato.\"Node\" where 1=1";
        String restartSeq = "ALTER SEQUENCE tomato.\"Node_iid_seq\" RESTART WITH 1;\n";
        try (Connection connection = JdbcDataSource.getConnection()){
            connection.prepareStatement(deleteNeighbour).execute();
            connection.prepareStatement(deleteNode).execute();
            connection.prepareStatement(restartSeq).execute();
        }
        EdgeRepository edgeRepository = new EdgeRepository();
        NodeRepository nodeRepository = new NodeRepository(edgeRepository);
        NodeCache nodeCache = new NodeCache(nodeRepository);
        NeighbourCache neighbourCache = new NeighbourCache(edgeRepository);
        LoadGraphToPlatform loadGraphToPlatform = new LoadGraphToPlatform(nodeCache,
            neighbourCache);
        int graphSize = loadGraphToPlatform.initialLoadFromTextFile("/input4");
        LinkedList<String> nodeQueue = new LinkedList<>();

        MessageCacheQueue messageCacheQueue = new MessageCacheQueue();
        int first = 0;
        int step = 50;

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
                    if(nodeQueue.isEmpty()){
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
                //nodeCache.flush("node%%", Node.class, nodeRepository, false);
            }
        }
    }
}
