package cache;

import GHS.Node;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import repository.NodeRepository;

public class NodeCache extends Cache<Node> {
    public NodeRepository nodeRepository = new NodeRepository();

    public void addNode(Node node) {
        try (Jedis jedis = pool.getResource()) {
            if (this.counter.get() % this.cacheSize == 0) {
                flush("node%%", Node.class, nodeRepository);
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

    public boolean exist(int id) {
        try (Jedis jedis = pool.getResource()) {
            String s = jedis.get("cc%%" + id);
            return s != null;
        }
    }
}
