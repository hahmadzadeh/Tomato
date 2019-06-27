package GHS;

import java.util.List;

public class Neighbour implements Comparable<Neighbour> {

    public static final byte REJECTED = 0X01;
    public static final byte BRANCH = 0X02;
    public static final byte BASIC = 0X03;

    public int source;
    public int destination;
    public Edge edge;
    public byte type = BASIC;

    public Neighbour(int sourceID, int destID, double weight) {
        this.source = sourceID;
        this.destination = destID;
        this.edge = new Edge(weight, sourceID, destID);
    }

    public Neighbour(int sourceID, int destID, double weight, byte type) {
        this.source = sourceID;
        this.destination = destID;
        this.type = type;
        this.edge = new Edge(weight, sourceID, destID);
    }

    public Neighbour() {

    }

    public static Neighbour getNeighbourById(int id, List<Neighbour> neighborList) {
        for (Neighbour n : neighborList) {
            if (n.destination == id) {
                return n;
            }
        }
        return null;
    }

    @Override
    public int compareTo(Neighbour o) {
        if (o.type == BASIC && type == BASIC) {
            return edge.compareTo(o.edge);
        } else {
            if (type == BASIC) {
                return -1;
            }
            if (o.type == BASIC) {
                return 1;
            }
            return edge.compareTo(o.edge);
        }
    }

    @Override
    public String toString() {
        return "{" + " source='" + source + "'" + ", destination='" + destination + "'" + ", edge='"
            + edge.weight
            + "'" + ", type='" + getTypeName() + "'" + "}";
    }

    private String getTypeName() {
        switch (type) {
            case BASIC:
                return "Basic";
            case BRANCH:
                return "Branch";
            case REJECTED:
                return "Rejected";
            default:
                return "none";
        }
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null){
            return false;
        }
        Neighbour neighbour = (Neighbour) obj;
        return neighbour.source == source && neighbour.destination == destination;
    }
}