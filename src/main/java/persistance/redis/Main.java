package persistance.redis;

import GHS.Message;
import cache.MessageCacheQueue;
import cache.MessageQueue;


public class Main {
    public static void main(String[] args) {
        MessageQueue queue = new MessageCacheQueue();
        queue.push(1, new Message(1, 2, Message.ACCEPT, (int) System.currentTimeMillis()));
    }
}
