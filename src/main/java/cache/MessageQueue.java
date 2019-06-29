package cache;

import GHS.Message;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public interface MessageQueue {

    Message pop(int id);

    void push(int id, Message message, boolean isMessageNew);

    Message peek(int id);

    int size(int id);

    Queue<Message> getAll(int id);

}
