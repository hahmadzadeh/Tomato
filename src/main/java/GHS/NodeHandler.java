package GHS;

import cache.MessageQueue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class NodeHandler implements Runnable {

    private static NodeHandler singleton;
    private ConcurrentHashMap<Integer, Node> nodesPool = new ConcurrentHashMap<>();
    private MessageQueue msgQueue;
    private ConcurrentHashMap<Integer, Boolean> hasNewMessage = new ConcurrentHashMap<>();

    private NodeHandler() {
        msgQueue = new MessageQueue() {
            public HashMap<Integer, ConcurrentLinkedQueue<Message>> map = new HashMap<>();

            @Override
            public int size(int id) {
                ConcurrentLinkedQueue<Message> messages = map.get(id);
                if (messages == null) {
                    return -1;
                }
                return messages.size();
            }

            @Override
            public void push(int id, Message message, boolean isMessageNew) {
                ConcurrentLinkedQueue<Message> messages = map.get(id);
                // System.out.println("receiver: " + id + ", new? " + isMessageNew + ", message:
                // " + message);
                if (messages == null) {
                    messages = addNewNode(id);
                }
                messages.add(message);
                if (isMessageNew) {
                    NodeHandler.getNodeHandler().hasNewMessage(id);
                }
            }

            @Override
            public Message pop(int id) {
                ConcurrentLinkedQueue<Message> messages = map.get(id);
                if (messages == null) {
                    return null;
                }

                return messages.poll();
            }

            @Override
            public Message peek(int id) {
                ConcurrentLinkedQueue<Message> messages = map.get(id);
                if (messages == null) {
                    return null;
                }

                return messages.peek();
            }

            @Override
            public LinkedList<Message> getAll(int id) {
                ConcurrentLinkedQueue<Message> messages = map.get(id);
                if (messages == null) {
                    return new LinkedList<>();
                }
                LinkedList<Message> result = new LinkedList<>();
                while (messages.peek() != null) {
                    result.add(messages.poll());
                }
                return result;
            }

            public ConcurrentLinkedQueue<Message> addNewNode(int id) {
                ConcurrentLinkedQueue<Message> mLinkedList = new ConcurrentLinkedQueue<Message>();
                map.put(id, mLinkedList);
                return mLinkedList;
            }
        };
    }

    public void hasNewMessage(int id) {
        if (hasNewMessage.get(id) != null) {
            hasNewMessage.replace(id, true);
        } else {
            hasNewMessage.put(id, true);
        }
        return;
    }

    public static NodeHandler getNodeHandler() {
        if (singleton == null) {
            singleton = new NodeHandler();
        }
        return singleton;
    }

    public Node getNodeById(int id) {
        return nodesPool.get(id);
    }

    public boolean insertToPool(Node node) {
        return (nodesPool.putIfAbsent(node.id, node) == null);
    }

    public Node constructNode(int id) {
        ArrayList<Edge> edges = EdgeHandler.getInstance().getEdgesGivenId(id);
        int[] neighbours = new int[edges.size()];
        double[] weights = new double[edges.size()];

        for (int i = 0; i < edges.size(); i++) {
            Edge edge = edges.get(i);
            weights[i] = edge.weight;
            neighbours[i] = (edge.biggerID == id) ? edge.smallerID : edge.biggerID;
        }

        Node node = new Node(id, neighbours, weights, msgQueue);
        insertToPool(node);
        msgQueue.addNewNode(id);
        return node;
    }

    @Override
    public void run() {
        while (true) {
            for (Node node : nodesPool.values()) {
                if (!node.isRunning) {
                    if (hasNewMessage.get(node.id) != null && hasNewMessage.get(node.id) == true) {
                        //Thread tr = new Thread(node);
                        hasNewMessage.replace(node.id, false);
                        //tr.start();
                    }
                }
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                //e.printStackTrace();
            }
        }
    }

    public void printEdges(Node node) {
        for (Neighbour n : node.neighbours) {
            if (n.type == Neighbour.BRANCH && n != node.inBranch) {
                System.out.println("(" + n.source + ", " + n.destination + ") ");
                printEdges(getNodeById(n.destination));
            }
        }
        if (node.id == getNodeById(node.inBranch.destination).inBranch.destination) {
            if (node.id < node.inBranch.destination) {
                System.out.println("(" + node.id + ", " + node.inBranch.destination + ") ");
            }
        }
    }

    public void printAll() {
        System.out.println("Ans :");
        for (Node n : nodesPool.values()) {
            System.out.println(n);
            System.out.println(" ");
        }
        System.out.println("End Ans");
    }
}