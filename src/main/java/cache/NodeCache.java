package cache;

import GHS.Node;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.IOException;

import redis.clients.jedis.Jedis;
import repository.NodeRepository;
import utils.RedisDataSource;

public class NodeCache extends Cache<Node> {
    public NodeRepository nodeRepository;

    public NodeCache(NodeRepository nodeRepository) {
        super();
        this.nodeRepository = nodeRepository;
    }

    public void addNode(Node node, boolean isnew) {
        try (Jedis jedis = RedisDataSource.getResource()) {
            if (this.counter.get() % this.cacheSize == 0) {
                flush("node%%", Node.class, nodeRepository, isnew);
            }
            this.counter.incrementAndGet();
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
            String s = jedis.get("cc%%" + id);
            return s != null;
        }
    }

    public void addExistence(int id) {
        try(Jedis jedis = RedisDataSource.getResource()){
            jedis.set("cc%%" + id, "1");
        }
    }
}
