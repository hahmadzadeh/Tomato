import GHS.Node;
import cache.MessageCacheQueue;
import cache.NeighbourCache;
import cache.NodeCache;
import java.io.IOException;
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

public class Main {

    public static void main(String[] args)
        throws IOException, SQLException, ExecutionException, InterruptedException {
        JedisPool pool = MessageCacheQueue.jedisPool;
        EdgeRepository edgeRepository = new EdgeRepository();
        NodeRepository nodeRepository = new NodeRepository(edgeRepository);
        NodeCache nodeCache = new NodeCache(nodeRepository);
        NeighbourCache neighbourCache = new NeighbourCache(edgeRepository);
        LoadGraphToPlatform loadGraphToPlatform = new LoadGraphToPlatform(nodeCache,
            neighbourCache);
        loadGraphToPlatform.initialLoadFromTextFile("/input4");
        LinkedList<String> nodeQueue = new LinkedList<>();
        MessageCacheQueue messageCacheQueue = new MessageCacheQueue();
        int first = 0;
        int step = 4;
        int graphSize = 11;
        int numThreads = 1;
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        List<Future<Node>> slavesResult = new LinkedList<>();
        while (true) {
            try (Jedis jedis = pool.getResource()) {
                for (int i = 0; i < numThreads; i++) {
                    if (nodeQueue.isEmpty()) {
                        nodeCache.flush("node%%", Node.class, nodeRepository, false);
                        nodeRepository.loadTrivial(first, first + step);
                        nodeQueue.addAll(jedis.keys("node%%*"));
                        first += step;
                        first %= graphSize;
                    }
                    String key = nodeQueue.poll();
                    Node node = nodeCache.getNode(key);
                    node.msgQueue = messageCacheQueue;
                    slavesResult.add(executorService.submit(node));
                }
                for (Future<Node> future : slavesResult) {
                    nodeCache.addNode(future.get());
                }
            }
        }
    }
}
