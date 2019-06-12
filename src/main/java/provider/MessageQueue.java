package provider;

import GHS.Message;

import java.util.List;

public interface MessageQueue {
    Message pop(int id);
    void push(int id, Message message);
    Message peek(int id);
    int size(int id);
    List<Message> getAll(int id);
}
