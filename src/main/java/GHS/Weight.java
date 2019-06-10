package GHS;

public class Weight implements Comparable<Weight> {
    public static final Weight INFINITY = new Weight(Double.MAX_VALUE, 0, 1);

    public double weight;
    public int smallerID;
    public int biggerID;

    public Weight(double weight, int id1, int id2) {
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
    public int compareTo(Weight o) { // ooni bozorg tare ke vaznesh bishtare
        Weight ow = (Weight) o;
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
        return "{" + " weight='" + weight + "'" + ", smallerID='" + smallerID + "'" + ", biggerID='" + biggerID + "'"
                + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof Weight)) {
            return false;
        }
        Weight mWeight = (Weight) o;
        return weight == mWeight.weight && smallerID == mWeight.smallerID && biggerID == mWeight.biggerID;
    }
}