package cache;

import GHS.Neighbour;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class NeighbourCache extends Cache{
    private JedisPool pool = MessageCacheQueue.jedisPool;
    private ObjectMapper mapper = new ObjectMapper();

    public void addNeighbour(Neighbour edge){
        try(Jedis jedis = pool.getResource()){
            jedis.rpush("edge%%" + edge.source % cacheSize, mapper.writeValueAsString(edge));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
    public Neighbour getNeighbour(int id){
        try (Jedis jedis = pool.getResource()){
            String s = jedis.get("edge%%" + id);
            return s == null ? null : mapper.readValue(s, Neighbour.class);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
