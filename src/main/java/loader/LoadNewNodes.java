package loader;

import GHS.Node;
import cache.NodeCache;
import redis.clients.jedis.Jedis;
import repository.NodeRepository;
import utils.RedisDataSource;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

public class LoadNewNodes implements Runnable {
    private LinkedBlockingQueue<String> inputQ;
    private LinkedBlockingQueue<Node> outputQ;
    private NodeRepository nodeRepository;
    private NodeCache nodeCache;
    private int first;
    private int step;
    private int cacheSize;
    private List<Node> expiredNode = new LinkedList<>();
    private Boolean done;
    private int graphSize;

    public LoadNewNodes(LinkedBlockingQueue<String> inputQ, LinkedBlockingQueue<Node> outputQ, NodeRepository nodeRepository
            , NodeCache nodeCache, int first, int step, int cacheSize, Boolean done, int graphSize) {
        this.inputQ = inputQ;
        this.outputQ = outputQ;
        this.nodeRepository = nodeRepository;
        this.nodeCache = nodeCache;
        this.first = first;
        this.step = step;
        this.cacheSize = cacheSize;
        this.done = done;
        this.graphSize = graphSize;
    }

    @Override
    public void run() {
        Set<Integer> candidate = null;
        try (Jedis jedis = RedisDataSource.getResource()){
            candidate = nodeRepository.loadTrivial(first, first + step, true);
            inputQ.addAll(jedis.keys("node%%*"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        List<String> tempList = new LinkedList<>();
        while (!done) {
            try (Jedis jedis = RedisDataSource.getResource()) {
                int currentCacheSize = jedis.keys("cache*").size();
                if (inputQ.size() <= step / 2) {
                    Set<Integer> collect = candidate.stream().filter(e -> !nodeCache.exist(e)).collect(Collectors.toSet());
                    candidate = nodeRepository.loadCandidates(collect);
                }
                if (currentCacheSize > cacheSize) {
                    int tempSize = currentCacheSize;
                    while (tempSize - cacheSize > cacheSize / 4) {
                        Node take = outputQ.poll();
                        if(take == null)
                            break;
                        expiredNode.add(take);
                        tempSize--;
                    }
                    nodeRepository.updateBatch(expiredNode, false);
                    expiredNode.forEach(e -> jedis.del("cache%%" + e.id));
                    expiredNode.forEach(e -> nodeCache.memCache.remove("node%%" + e.id));
                }else {
                    while (currentCacheSize < cacheSize){
                        Node take = outputQ.poll();
                        if(take == null)
                            break;
                        nodeCache.memCache.put("node%%" + take.id, take);
                        tempList.add("node%%" + take.id);
                        Collections.shuffle(tempList, new Random(2));
                        inputQ.addAll(tempList);
                        tempList.clear();
                        currentCacheSize++;
                    }
                }
                done = jedis.keys("finishNode%%*").size() == graphSize;
            } catch (SQLException  e) {
                e.printStackTrace();
            }
        }
    }
}
