package GHS;

public class Edge implements Comparable<Edge> {
    public static final Edge INFINITY = new Edge(Double.MAX_VALUE, 0, 1);

    public double weight;
    public int smallerID;
    public int biggerID;

    public Edge(double weight, int id1, int id2) {
        assert (id1 != id2);
        this.weight = weight;
        if (id1 < id2) {
            smallerID = id1;
            biggerID = id2;
        } else {
            smallerID = id2;
            biggerID = id1;
        }
    }

    @Override
    public int compareTo(Edge o) { // ooni bozorg tare ke vaznesh bishtare
        Edge ow = (Edge) o;
        if (ow.weight != weight) {
            return (weight - ow.weight) > 0 ? 1 : -1;
        }
        if (ow.smallerID != smallerID) {
            return smallerID - ow.smallerID;
        }
        return biggerID - ow.biggerID;
    }

    @Override
    public String toString() {
        return "{" + " edge='" + weight + "'" + ", smallerID='" + smallerID + "'" + ", biggerID='" + biggerID + "'"
                + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof Edge)) {
            return false;
        }
        Edge mEdge = (Edge) o;
        return weight == mEdge.weight && smallerID == mEdge.smallerID && biggerID == mEdge.biggerID;
    }
}