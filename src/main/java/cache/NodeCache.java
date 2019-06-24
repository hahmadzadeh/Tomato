package cache;

import GHS.Node;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class NodeCache extends Cache{
    private JedisPool pool = MessageCacheQueue.jedisPool;
    private ObjectMapper mapper = new ObjectMapper();

    public void addNode(Node node){
        try(Jedis jedis = pool.getResource()){
            if(this.counter.incrementAndGet() % this.cacheSize == 0){

            }
            jedis.set("node%%" + node.id % cacheSize, mapper.writeValueAsString(node));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
    public Node getNode(int id){
        try (Jedis jedis = pool.getResource()){
            String s = jedis.get("node%%" + id);
            return s == null ? null : mapper.readValue(s, Node.class);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
