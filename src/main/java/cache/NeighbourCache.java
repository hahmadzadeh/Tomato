package cache;

import GHS.Neighbour;
import GHS.Node;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import repository.EdgeRepository;

public class NeighbourCache extends Cache<Neighbour>{
    public EdgeRepository edgeRepository = new EdgeRepository();

    public void addNeighbour(Neighbour edge){
        try(Jedis jedis = pool.getResource()){
            if (this.counter.get() % this.cacheSize == 0) {
                flush("edge%%", Neighbour.class, edgeRepository);
            }
            jedis.rpush("edge%%" + edge.source, mapper.writeValueAsString(edge));
            this.counter.incrementAndGet();
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
