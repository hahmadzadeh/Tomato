package persistance.redis;

import redis.clients.jedis.Jedis;

public class Main {
    public static void main(String[] args) {
        Jedis jedis = new Jedis();
        jedis.lpush("helo#t", "world!!!");
    }
}
