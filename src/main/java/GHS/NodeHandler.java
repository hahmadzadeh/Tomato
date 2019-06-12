package GHS;

import java.util.ArrayList;
import java.util.HashMap;

public class NodeHandler {
    private static NodeHandler singleton;
    private HashMap<Integer, Node> nodesPool = new HashMap<>();

    private NodeHandler() {

    }

    public static NodeHandler getNodeHandler() {
        if (singleton == null)
            singleton = new NodeHandler();
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

        Node node = new Node(id, neighbours, weights);
        insertToPool(node);
        return node;
    }
}