package cache;

import GHS.Node;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.IOException;

import redis.clients.jedis.Jedis;
import repository.NodeRepository;

public class NodeCache extends Cache<Node> {
    public NodeRepository nodeRepository;

    public NodeCache(NodeRepository nodeRepository) {
        super();
        this.nodeRepository = nodeRepository;
    }

    public void addNode(Node node) {
        try (Jedis jedis = pool.getResource()) {
            if (this.counter.get() % this.cacheSize == 0) {
                flush("node%%", Node.class, nodeRepository, true);
            }
            this.counter.incrementAndGet();
            jedis.set("node%%" + node.id, mapper.writeValueAsString(node));
            jedis.set("cc%%" + node.id, "1");
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    public Node getNode(int id) {
        try (Jedis jedis = pool.getResource()) {
            String s = jedis.get("node%%" + id);
            return s == null ? null : mapper.readValue(s, Node.class);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public Node getNode(String id) {
        try (Jedis jedis = pool.getResource()) {
            String s = jedis.get(id);
            jedis.del(id);
            return s == null ? null : mapper.readValue(s, Node.class);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public boolean exist(int id) {
        try (Jedis jedis = pool.getResource()) {
            String s = jedis.get("cc%%" + id);
            return s != null;
        }
    }
}
