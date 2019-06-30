package cache;

import GHS.Node;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import redis.clients.jedis.Jedis;
import repository.NodeRepository;
import utils.RedisDataSource;

public class NodeCache extends Cache<Node> {
    public NodeRepository nodeRepository;
    public ConcurrentHashMap<String, Node> memCache;

    public NodeCache(NodeRepository nodeRepository) {
        super();
        this.nodeRepository = nodeRepository;
        memCache = new ConcurrentHashMap<>();
    }

    public void addNode(Node node, boolean isnew) {
        try (Jedis jedis = RedisDataSource.getResource()) {
            //this.counter.incrementAndGet();
            jedis.set("node%%" + node.id, mapper.writeValueAsString(node));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    public Node getNode(int id) {
        try (Jedis jedis = RedisDataSource.getResource()) {
            String s = jedis.get("node%%" + id);
            return s == null ? null : mapper.readValue(s, Node.class);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public Node getNode(String id) {
        try (Jedis jedis = RedisDataSource.getResource()) {
            Node mmc = memCache.get(id);
            if(mmc != null){
                memCache.remove(id);
                return mmc;
            }
            String s = jedis.get(id);
            jedis.del(id);
            return s == null ? null : mapper.readValue(s, Node.class);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public boolean exist(int id) {
        try (Jedis jedis = RedisDataSource.getResource()) {
            String s = jedis.get("cache%%" + id);
            return s != null;
        }
    }

    public void addExistence(int id) {
        try(Jedis jedis = RedisDataSource.getResource()){
            jedis.set("cc%%" + id, "1");
        }
    }

    public void flushCache(LinkedBlockingQueue<String> inputQ, LinkedBlockingQueue<Node> outputQ) throws SQLException {
        List<Node> list = new LinkedList<>();
        inputQ.forEach(e -> list.add(this.getNode(e)));
        list.addAll(outputQ);
        nodeRepository.updateBatch(list, false);
    }
}
