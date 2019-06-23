package repository;

import GHS.Edge;
import GHS.Neighbour;
import GHS.Node;
import utils.JdbcDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class NodeRepository implements Repository<Node> {
    private final String insertNodeDdl = "INSERT INTO tomato.\"Node\" (ID, BEST_EDGE, TEST_EDGE, IN_BRANCH, LEVEL, FIND_COUNT, STATE, FRAGMENT_ID,  BEST_WEIGHT) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private final String updateDdl = "UPDATE tomato.\"Node\"\n" +
            "\tSET id=?, best_edge=?, test_edge=?, in_branch=?, level=?, find_count=?, state=?, fragment_id=?, best_weight=?\n" +
            "\tWHERE id=?;";

    private final String selectNodesTrivialDdl = "SELECT * FROM tomato.\"Node\" WHERE id BETWEEN ? AND ?";
    private final String selectEdgeTrivialDdl = "SELECT * FROM tomato.\"Neighbour\" WHERE source BETWEEN ? AND ?";

    @Override
    public void save(Node entity) throws SQLException {
        try (Connection connection = JdbcDataSource.getConnection(); PreparedStatement statement = connection.prepareStatement
                (insertNodeDdl)) {
            statement.setInt(1, entity.id);
            statement.setInt(2, entity.bestEdge != null ? entity.bestEdge.destination : -1);
            statement.setInt(3, entity.testEdge != null ? entity.testEdge.destination : -1);
            statement.setInt(4, entity.inBranch != null ? entity.inBranch.destination : -1);
            statement.setInt(5, entity.level);
            statement.setInt(6, entity.findCount);
            statement.setByte(7, entity.state);
            statement.setString(8, entity.fragmentId != null ? entity.fragmentId.toString() : "");
            statement.setString(9, entity.bestWeight != null ? entity.bestWeight.toString() : "");
        }
    }

    @Override
    public void update(Node entity) throws SQLException {
        try (Connection connection = JdbcDataSource.getConnection(); PreparedStatement statement = connection.prepareStatement
                (updateDdl)) {
            statement.setInt(1, entity.id);
            statement.setInt(2, entity.bestEdge != null ? entity.bestEdge.destination : -1);
            statement.setInt(3, entity.testEdge != null ? entity.testEdge.destination : -1);
            statement.setInt(4, entity.inBranch != null ? entity.inBranch.destination : -1);
            statement.setInt(5, entity.level);
            statement.setInt(6, entity.findCount);
            statement.setByte(7, entity.state);
            statement.setString(8, entity.fragmentId != null ? entity.fragmentId.serialized() : "");
            statement.setString(9, entity.bestWeight != null ? entity.bestWeight.serialized() : "");
            statement.setInt(10, entity.id);
            statement.execute();
        }
    }

    @Override
    public List<Node> loadTrivial(int first, int last) throws SQLException {
        try (Connection connection = JdbcDataSource.getConnection(); PreparedStatement psNode = connection.prepareStatement
                (selectNodesTrivialDdl); PreparedStatement psEdge = connection.prepareStatement(selectEdgeTrivialDdl)) {
            psNode.setInt(1, first);
            psNode.setInt(2, last);
            psEdge.setInt(1, first);
            psEdge.setInt(2, last);
            ResultSet resultSet = psEdge.executeQuery();
            HashMap<Integer, HashMap<Integer, Neighbour>> neighbours = new HashMap<>();
            while (resultSet.next()) {
                int source = resultSet.getInt("source");
                int dest = resultSet.getInt("dest");
                double weight = resultSet.getDouble("weight");
                byte type = resultSet.getByte("type");
                Neighbour neighbour = new Neighbour(source, dest, weight, type);
                neighbours.computeIfAbsent(source, k -> new HashMap());
                neighbours.get(source).put(dest, neighbour);
            }
            resultSet = psNode.executeQuery();
            List<Node> nodes = new LinkedList<>();
            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                int best_edge = resultSet.getInt("best_edge");
                int test_edge = resultSet.getInt("test_edge");
                int in_branch = resultSet.getInt("in_branch");
                int level = resultSet.getInt("level");
                int find_count = resultSet.getInt("find_count");
                byte state = resultSet.getByte("state");
                Edge fragment_id = Edge.buildFromString(resultSet.getString("fragment_id"));
                Edge best_weight = Edge.buildFromString(resultSet.getString("best_weight"));
                HashMap<Integer, Neighbour> neighbourHashMap = neighbours.get(id);
                nodes.add(new Node(id, new LinkedList<>(neighbourHashMap.values()), state, neighbourHashMap.getOrDefault(best_edge, null),
                        neighbourHashMap.getOrDefault(test_edge, null),
                        neighbourHashMap.getOrDefault(in_branch, null), best_weight, find_count, level, fragment_id));
            }
            return nodes;
        }
    }

    @Override
    public void saveBatch(List<Node> listEntity) throws SQLException {
        try (Connection connection = JdbcDataSource.getConnection(); PreparedStatement statement = connection.prepareStatement
                (insertNodeDdl)) {
            for (Node entity : listEntity) {
                statement.setInt(1, entity.id);
                statement.setInt(2, entity.bestEdge != null ? entity.bestEdge.destination : -1);
                statement.setInt(3, entity.testEdge != null ? entity.testEdge.destination : -1);
                statement.setInt(4, entity.inBranch != null ? entity.inBranch.destination : -1);
                statement.setInt(5, entity.level);
                statement.setInt(6, entity.findCount);
                statement.setByte(7, entity.state);
                statement.setString(8, entity.fragmentId != null ? entity.fragmentId.toString() : "");
                statement.setString(9, entity.bestWeight != null ? entity.bestWeight.toString() : "");
                statement.addBatch();
            }
        }
    }
}
