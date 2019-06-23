package GHS;

import java.util.ArrayList;

public class EdgeHandler {
    private static EdgeHandler singleton;
    private ArrayList<Edge> edges = new ArrayList<>();

    private EdgeHandler() {

    }

    public static EdgeHandler getInstance() {
        if (singleton == null)
            synchronized (EdgeHandler.class) {
                singleton = new EdgeHandler();
            }
        return singleton;
    }

    public void addEdge(int id1, int id2, double weight) {
        if (id1 != id2 && weight >= 0 && optionalCheck(id1, id2))
            edges.add(new Edge(weight, id1, id2));
    }

    private boolean optionalCheck(int id1, int id2) {
        for (Edge edge : edges) {
            if ((edge.biggerID == id1 && edge.smallerID == id2) || (edge.biggerID == id2 && edge.smallerID == id1))
                return false;
        }
        return true;
    }

    public ArrayList<Edge> getEdgesGivenId(int id) {

        ArrayList<Edge> result = new ArrayList<>();
        for (Edge edge : edges) {
            if (edge.biggerID == id || edge.smallerID == id)
                result.add(edge);
        }
        return result;
    }
}