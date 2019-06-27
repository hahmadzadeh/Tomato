package cache;

import GHS.Message;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MessageCacheQueue extends Cache implements MessageQueue {

    public final static JedisPoolConfig poolConfig = buildPoolConfig();
    public final static JedisPool jedisPool = new JedisPool(poolConfig, "localhost");
    private ObjectMapper mapper = new ObjectMapper();

    private static JedisPoolConfig buildPoolConfig() {
        final JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(128);
        poolConfig.setMaxIdle(128);
        poolConfig.setMinIdle(16);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setMinEvictableIdleTimeMillis(Duration.ofSeconds(60).toMillis());
        poolConfig.setTimeBetweenEvictionRunsMillis(Duration.ofSeconds(30).toMillis());
        poolConfig.setNumTestsPerEvictionRun(3);
        poolConfig.setBlockWhenExhausted(true);
        return poolConfig;
    }

    @Override
    public Message pop(int id) {
        try (Jedis jedis = jedisPool.getResource()) {
            try {
                String rpop = jedis.lpop("msg%%" + id);
                jedis.lpush("dMsg%%" + id, rpop);
                System.out.println(rpop);
                return rpop == null ? null : mapper.readValue(rpop, Message.class);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    @Override
    public void push(int id, Message message, boolean isNew) {
        try (Jedis jedis = jedisPool.getResource()) {
            try {
                jedis.rpush("msg%%" + id, mapper.writeValueAsString(message));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public Message peek(int id) {
        try (Jedis jedis = jedisPool.getResource()) {
            try {
                return jedis.lrange("msg%%" + id, -1, -1).size() == 0 ? null :
                        mapper.readValue(jedis.lrange("msg%%" + id, -1, -1).get(0), Message.class);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    @Override
    public int size(int id) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.lrange("msg%%" + id, 0, -1).size();
        }
    }

    @Override
    public Queue<Message> getAll(int id) {
        try (Jedis jedis = jedisPool.getResource()) {
            Stream<String> stream = jedis.lrange("msg%%" + id, 0, -1).stream();
            List<String> ids = stream.collect(Collectors.toList());
            ids.forEach(e -> jedis.rpush("dmgs%%" + id, e));
            List<Message> collect = ids
                    .stream().map(e -> {
                        try {
                            return mapper.readValue(e, Message.class);
                        } catch (IOException e1) {
                            e1.printStackTrace();
                        }
                        return null;
                    }).filter(Objects::nonNull).collect(Collectors.toList());
            jedis.del("msg%%" + id);
            return new LinkedBlockingQueue<>(collect);
        }
    }

    @Override
    public ConcurrentLinkedQueue<Message> addNewNode(int id) {
        return null;
    }
}
