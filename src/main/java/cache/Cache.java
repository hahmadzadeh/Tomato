package cache;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import GHS.Node;
import com.fasterxml.jackson.databind.ObjectMapper;
import loader.LoadGraphToPlatform;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import repository.Repository;

public class Cache<T> {

    protected Properties properties;
    protected int cacheSize;
    protected int cacheSizeMsg;
    public AtomicInteger counter;
    protected JedisPool pool = MessageCacheQueue.jedisPool;
    protected ObjectMapper mapper = new ObjectMapper();

    public Cache() {
        this.properties = new Properties();
        try (
                InputStream in = LoadGraphToPlatform.class.getResourceAsStream("/config.properties")) {
            System.out.println(in);
            properties.load(in);
            this.cacheSize = Integer.parseInt(properties.getProperty("cacheSize"));
            this.cacheSizeMsg = Integer.parseInt(properties.getProperty("cacheSizeMsg"));
            this.counter = new AtomicInteger(1);
        } catch (
                IOException e) {
        }
    }

    public void flush(String keyPrefix, Class<T> tClass, Repository<T> repository , boolean isnew) {
        synchronized (this) {
            if (this.counter.get() % this.cacheSize == 0) {
                try (Jedis jedis = pool.getResource()) {
                    List<T> entityList = new LinkedList<>();
                    Set<String> keys = jedis.keys(keyPrefix + "*");
                    for (String key: keys) {
                        if (keyPrefix.equals("node%%")) {
                            String entityString = jedis.get(key);
                            T entity = mapper.readValue(entityString, tClass);
                            entityList.add(entity);
                        } else {
                            List<String> listEntity = jedis.lrange(key, 0, -1);
                            for (String entityString : listEntity) {
                                T entity = mapper.readValue(entityString, tClass);
                                entityList.add(entity);
                            }
                        }
                        jedis.del(key);
                    }
                    if(isnew)
                        repository.saveBatch(entityList);
                    else
                        repository.updateBatch(entityList);
                    this.counter.incrementAndGet();
                } catch (IOException | SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
