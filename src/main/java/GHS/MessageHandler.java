package GHS;

import java.util.ArrayList;
import java.util.Queue;;

public class MessageHandler {
    private static MessageHandler singleton;

    private MessageHandler() {
    }

    public static MessageHandler getMessageHandler() {
        if (singleton == null)
            singleton = new MessageHandler();
        return singleton;
    }

    public void transferMessage(Message msg) {
        System.out.println(msg);
        Node receiver = NodeHandler.getNodeHandler().getNodeById(msg.receiverID);
        if (receiver == null) {
            // TODO must fetch node
        } else {
            receiver.messages.add(msg);
            receiver.hasNewMessages = true;
            if (!receiver.isRunning) {
                Thread tr = new Thread(receiver);
                tr.start();
            }
        }
    }
}