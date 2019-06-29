package cache;

import GHS.Neighbour;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import redis.clients.jedis.Jedis;
import repository.EdgeRepository;
import utils.RedisDataSource;

public class NeighbourCache extends Cache<Neighbour>{

    public NeighbourCache(EdgeRepository edgeRepository) {
        super();
        this.edgeRepository = edgeRepository;
    }

    public EdgeRepository edgeRepository;

    public void addNeighbour(Neighbour edge){
        try(Jedis jedis = RedisDataSource.getResource()){
            if (this.counter.get() % this.cacheSize == 0) {
                flush("edge%%", Neighbour.class, edgeRepository, true);
            }
            jedis.rpush("edge%%" + edge.source, mapper.writeValueAsString(edge));
            this.counter.incrementAndGet();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
    public Neighbour getNeighbour(int id){
        try (Jedis jedis = RedisDataSource.getResource()){
            String s = jedis.get("edge%%" + id);
            return s == null ? null : mapper.readValue(s, Neighbour.class);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
