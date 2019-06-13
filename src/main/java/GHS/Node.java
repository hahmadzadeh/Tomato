package GHS;

import com.fasterxml.jackson.annotation.JsonIgnore;
import cache.MessageQueue;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Node implements Runnable {
    public static final byte SLEEPING = 0X01;
    public static final byte FOUND = 0X02;
    public static final byte FIND = 0X03;

    @JsonIgnore
    public Queue<Message> messages = new ConcurrentLinkedQueue<>();
    @JsonIgnore
    private Queue<Message> capturedMessages = new LinkedList<>();
    @JsonIgnore
    private MessageQueue queue;
    @JsonIgnore
    private Queue<Message> returnedMessages = new LinkedList<>();
    @JsonIgnore
    public boolean hasNewMessages = true;
    @JsonIgnore
    public boolean isRunning = false;

    public int id;
    @JsonIgnore
    public List<Neighbour> neighbours;

    public byte state = SLEEPING;
    public Neighbour bestEdge;
    public Neighbour testEdge;
    public Neighbour inBranch;
    public Edge bestWeight;
    public int findCount;

    public int level = 0;
    public Edge fragmentId;

    public Node(int id, int[] neighbours, double[] weights, MessageQueue queue) {
        this.queue = queue;
        assert (neighbours.length == weights.length);
        this.id = id;
        Neighbour[] arrayOfNeighbours = new Neighbour[neighbours.length];
        for (int i = 0; i < neighbours.length; i++) {
            arrayOfNeighbours[i] = new Neighbour(id, neighbours[i], weights[i]);
        }
        this.neighbours = Arrays.asList(arrayOfNeighbours);
    }

    @Override
    public void run() {
        Thread.currentThread().setName("node");
        isRunning = true;

        if (state == SLEEPING) {
            wakeup();
        }

        while (hasNewMessages) {
            assert (capturedMessages.size() == 0);

            while (queue.peek(this.id) != null) {
                capturedMessages.add(queue.pop(this.id));
            }

            while (returnedMessages.peek() != null) {
                capturedMessages.add(returnedMessages.poll());
            }

            //hasNewMessages = false;

            boolean canContinue = true;
            while (canContinue) {
                int beforeExecSize = capturedMessages.size();
                while (capturedMessages.peek() != null) {
                    Message msg = capturedMessages.poll();
                    assert (msg.receiverID == id);

                    System.out.println("id " + id + ": executing " + msg.timestamp);

                    switch (msg.type) {
                    case Message.ACCEPT:
                        receiveAccept(msg.senderID);
                        break;
                    case Message.REJECT:
                        receiveReject(msg.senderID);
                        break;
                    case Message.CHANGE_CORE:
                        receiveChangeCore();
                        break;
                    case Message.CONNECT:
                        receivedConnect(msg.level, msg.senderID, msg);
                        break;
                    case Message.TEST:
                        receiveTest(msg.level, msg.fragmentID, msg.senderID, msg);
                        break;
                    case Message.INITIATE:
                        receivedInitiate(msg.level, msg.fragmentID, msg.state, msg.senderID);
                        break;
                    case Message.REPORT:
                        receiveReport(msg.edge, msg.senderID, msg);
                        break;
                    }
                }
                int afterExecSize = returnedMessages.size();
                canContinue = (afterExecSize < beforeExecSize) ? true : false;
                if (canContinue) {
                    while (returnedMessages.peek() != null) {
                        capturedMessages.add(returnedMessages.poll());
                    }
                }
            }
        }
        isRunning = false;
    }

    public void wakeup() {
        Collections.sort(this.neighbours);
        if (this.neighbours.size() == 0) {
            finish();
            return;
        }

        Neighbour m = this.neighbours.get(0);
        assert (m != null);

        m.type = Neighbour.BRANCH;
        level = 0;
        state = FOUND;
        findCount = 0;

        sendConnect(m);
    }

    public void receivedConnect(int senderLevel, int neighbourID, Message inMsg) {
        Neighbour sender = Neighbour.getNeighbourById(neighbourID, this.neighbours);
        assert (sender != null) : "wrong message. neighbour not foud.";
        if (state == SLEEPING)
            wakeup();
        if (senderLevel < level) {
            sender.type = Neighbour.BRANCH;
            sendInitiate(sender, true);
            if (state == FIND)
                findCount++;
        } else if (sender.type == Neighbour.BASIC) {
            returnedMessages.add(inMsg);
        } else {
            sendInitiate(sender, false);
        }
    }

    public void receivedInitiate(int senderLevel, Edge senderFragmentId, byte senderState, int neighbourID) {
        Neighbour sender = Neighbour.getNeighbourById(neighbourID, this.neighbours);
        assert (sender != null) : "wrong message. neighbour not foud.";
        assert (state != SLEEPING) : "should not receive in this state";
        this.state = senderState;
        this.level = senderLevel;
        this.fragmentId = senderFragmentId;
        inBranch = sender;
        bestEdge = null;
        bestWeight = Edge.INFINITY;
        for (Neighbour neighbour : neighbours) {
            if (neighbour.type != Neighbour.BRANCH)
                continue;
            if (neighbour == sender)
                continue;
            sendInitiate(neighbour, true);
            if (state == FIND)
                findCount++;
        }

        if (state == FIND)
            test();
    }

    public void test() {
        Collections.sort(this.neighbours);
        Neighbour m = this.neighbours.get(0);
        assert (m != null);

        if (m.type != Neighbour.BASIC) {
            testEdge = null;
            report();
            return;
        }
        testEdge = m;
        sendTest(m);

    }

    public void receiveTest(int senderLevel, Edge senderFragmentId, int neighbourID, Message inMsg) {
        Neighbour sender = Neighbour.getNeighbourById(neighbourID, this.neighbours);
        assert (sender != null) : "wrong message. neighbour not foud.";

        if (state == SLEEPING)
            wakeup();
        if (level < senderLevel) {
            returnedMessages.add(inMsg);
        } else if (!this.fragmentId.equals(senderFragmentId)) {
            sendAccept(sender);
        } else {
            if (sender.type == Neighbour.BASIC)
                sender.type = Neighbour.REJECTED;
            if (testEdge != sender) {
                sendReject(sender);
            } else {
                test();
            }
        }
    }

    public void receiveAccept(int neighbourID) {
        Neighbour sender = Neighbour.getNeighbourById(neighbourID, this.neighbours);
        assert (sender != null) : "wrong message. neighbour not foud.";

        testEdge = null;
        if (bestWeight.compareTo(sender.edge) > 0) {
            bestEdge = sender;
            bestWeight = sender.edge;
            report();
        }
    }

    public void receiveReject(int neighbourID) {
        Neighbour sender = Neighbour.getNeighbourById(neighbourID, this.neighbours);
        assert (sender != null) : "wrong message. neighbour not foud.";

        if (sender.type == Neighbour.BASIC) {
            sender.type = Neighbour.REJECTED;
            test();
        }
    }

    public void report() {
        if (findCount == 0 && testEdge == null) {
            state = FOUND;
            sendReport(bestWeight);
        }
    }

    public void receiveReport(Edge edge, int neighbourID, Message inMsg) {
        Neighbour sender = Neighbour.getNeighbourById(neighbourID, this.neighbours);
        assert (sender != null) : "wrong message. neighbour not foud.";

        if (sender != inBranch) {
            findCount--;
            if (bestWeight.compareTo(edge) > 0) {
                bestWeight = edge;
                bestEdge = sender;
            }
            report();
        } else if (state == FIND) {
            returnedMessages.add(inMsg);
        } else if (bestWeight.compareTo(edge) < 0) {
            changeCore();
        } else if (edge.compareTo(bestWeight) == 0 && edge.compareTo(Edge.INFINITY) == 0) {
            finish();
        }
    }

    private void changeCore() {
        if (bestEdge.type == Neighbour.BRANCH) {
            sendChangeCore(bestEdge);
        } else {
            sendConnect(bestEdge);
            bestEdge.type = Neighbour.BRANCH;
        }
    }

    public void receiveChangeCore() {
        changeCore();
    }

    private void sendChangeCore(Neighbour bestEdge2) {
        Message msg = new Message(id, bestEdge2.destination, Message.CHANGE_CORE,  System.currentTimeMillis());
        queue.push(bestEdge2.destination, msg);
    }

    private void sendReport(Edge bestEdge) {
        assert (inBranch != null);
        Message msg = new Message(id, inBranch.destination, Message.REPORT,  System.currentTimeMillis());
        msg.edge = bestEdge;
        queue.push(inBranch.destination, msg);
    }

    private void sendReject(Neighbour sender) {
        Message msg = new Message(id, sender.destination, Message.REJECT, System.currentTimeMillis());
        queue.push(sender.destination, msg);
    }

    private void sendAccept(Neighbour sender) {
        Message msg = new Message(id, sender.destination, Message.ACCEPT, System.currentTimeMillis());
        queue.push(sender.destination, msg);
    }

    private void sendTest(Neighbour m) {
        Message msg = new Message(id, m.destination, Message.TEST, System.currentTimeMillis());
        msg.level = level;
        msg.fragmentID = fragmentId;
        queue.push(m.destination, msg);
    }

    private void sendInitiate(Neighbour receiver, boolean selfInfo) {
        Message msg = new Message(id, receiver.destination, Message.INITIATE, System.currentTimeMillis());
        if (selfInfo) {
            msg.level = level;
            msg.fragmentID = fragmentId;
            msg.state = state;
        } else {
            msg.level = level + 1;
            msg.fragmentID = receiver.edge;
            msg.state = FIND;
        }
        queue.push(receiver.destination, msg);
    }

    private void sendConnect(Neighbour m) {
        Message msg = new Message(id, m.destination, Message.CONNECT, System.currentTimeMillis());
        msg.level = level;
        queue.push(m.destination, msg);
    }

    private void finish() {
        System.out.println("{ id: " + id + " ,inBranch: " + ((inBranch != null) ? inBranch.destination : null) + "}");
    }

    @Override
    public String toString() {
        return "{" + " messages='" + messages + "'" + ", capturedMessages='" + capturedMessages + "'"
                + ", returnedMessages='" + returnedMessages + "'" + ", hasNewMessages='" + hasNewMessages + "'"
                + ", isRunning='" + isRunning + "'" + ", id='" + id + "'" + ", neighbours='" + neighbours + "'"
                + ", state='" + getStateName(state) + "'" + ", bestEdge='" + bestEdge + "'" + ", testEdge='" + testEdge
                + "'" + ", inBranch='" + inBranch + "'" + ", bestEdge='" + bestEdge + "'" + ", findCount='"
                + findCount + "'" + ", level='" + level + "'" + ", fragmentId='" + fragmentId + "'" + "}";
    }

    public static String getStateName(byte state) {
        switch (state) {
        case FIND:
            return "Find";
        case FOUND:
            return "Found";
        case SLEEPING:
            return "Sleeping";
        default:
            return "none";
        }
    }

}