package cache;

import GHS.Message;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import redis.clients.jedis.Jedis;
import utils.RedisDataSource;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;


public class MessageCacheQueue extends Cache implements MessageQueue {
    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public Message pop(int id) {
        try (Jedis jedis = RedisDataSource.getResource()) {
            try {
                String rpop = jedis.lpop("msg%%" + id);
                return rpop == null ? null : mapper.readValue(rpop, Message.class);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    @Override
    public void push(int id, Message message, boolean isNew) {
        try (Jedis jedis = RedisDataSource.getResource()) {
            if (isNew)
                System.out.println(isNew + "---" + message);
            try {
                jedis.rpush("msg%%" + id, mapper.writeValueAsString(message));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public Message peek(int id) {
        try (Jedis jedis = RedisDataSource.getResource()) {
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
        try (Jedis jedis = RedisDataSource.getResource()) {
            return jedis.lrange("msg%%" + id, 0, -1).size();
        }
    }

    @Override
    public Queue<Message> getAll(int id) {
        LinkedBlockingQueue<Message> queue = new LinkedBlockingQueue<>();
        Message pop = pop(id);
        while (pop != null) {
            queue.add(pop);
            pop = pop(id);
        }
        return queue;
    }

}
