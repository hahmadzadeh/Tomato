package repository;

import GHS.Edge;
import GHS.Message;
import GHS.Neighbour;
import GHS.Node;
import cache.MessageCacheQueue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import utils.JdbcDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class NodeRepository implements Repository<Node> {

    private final JedisPool pool = MessageCacheQueue.jedisPool;
    private final String insertNodeDdl =
            "INSERT INTO tomato.\"Node\" (ID, BEST_EDGE, TEST_EDGE, IN_BRANCH, LEVEL, FIND_COUNT, STATE, FRAGMENT_ID,  BEST_WEIGHT) "
                    +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private final String updateDdl = "UPDATE tomato.\"Node\"\n" +
            "\tSET id=?, best_edge=?, test_edge=?, in_branch=?, level=?, find_count=?, state=?, fragment_id=?, best_weight=?\n"
            +
            "\tWHERE id=?;";

    private final String selectNodesTrivialDdl = "SELECT * FROM tomato.\"Node\" WHERE iid BETWEEN ? AND ?";
    private final String selectEdgeTrivialDdl = "SELECT * FROM tomato.\"Neighbour\" WHERE source IN (";
    private final String selectNodeDdl = "SELECT * FROM tomato.\"Node\" WHERE id=?";
    private final String selectEdgeDdl = "SELECT * FROM tomato.\"Neighbour\" WHERE source=?";
    private final ObjectMapper mapper = new ObjectMapper();
    private EdgeRepository edgeRepository;

    public NodeRepository(EdgeRepository edgeRepository){
        this.edgeRepository = edgeRepository;
    }


    @Override
    public void save(Node entity) throws SQLException {
        try (Connection connection = JdbcDataSource
                .getConnection(); PreparedStatement statement = connection.prepareStatement
                (insertNodeDdl)) {
            setNodeStatement(statement, entity);
            statement.execute();
        }
    }

    @Override
    public void update(Node entity) throws SQLException {
        try (Connection connection = JdbcDataSource
                .getConnection(); PreparedStatement statement = connection.prepareStatement
                (updateDdl)) {
            setNodeStatement(statement, entity);
            statement.setInt(10, entity.id);
            statement.execute();
        }
    }

    public void loadTrivial(int first, int last) throws SQLException {
        try (Connection connection = JdbcDataSource
                .getConnection(); PreparedStatement psNode = connection.prepareStatement
                (selectNodesTrivialDdl)) {
            psNode.setInt(1, first);
            psNode.setInt(2, last);
            ResultSet resultSet = psNode.executeQuery();
            List<TempNode> tempNodes = new LinkedList<>();
            while (resultSet.next()) {
                tempNodes.add(buildTempNodeFromResultSet(resultSet));
            }
            if(tempNodes.size() == 0){
                return;
            }
            StringBuilder sb = new StringBuilder();
            sb.append(selectEdgeTrivialDdl);
            for (int i = 0; i < tempNodes.size(); i++) {
                sb.append("?,");
            }
            String query = sb.deleteCharAt(sb.length() - 1).toString() + ")";
            PreparedStatement psEdge = connection.prepareStatement(query);
            for (int i = 0; i < tempNodes.size(); i++) {
                psEdge.setInt(i + 1, tempNodes.get(i).id);
            }
            resultSet = psEdge.executeQuery();
            HashMap<Integer, HashMap<Integer, Neighbour>> neighbours = new HashMap<>();
            while (resultSet.next()) {
                Neighbour neighbour = buildNeighbourFromResultSet(resultSet);
                neighbours.computeIfAbsent(neighbour.source, k -> new HashMap());
                neighbours.get(neighbour.source).put(neighbour.destination, neighbour);
            }
            List<Node> nodes = new LinkedList<>();
            for (TempNode tempNode : tempNodes) {
                nodes.add(buildNodeFromTempNode(tempNode, neighbours.get(tempNode.id)));
            }
            try (Jedis jedis = pool.getResource()) {
                nodes.forEach(e -> {
                    try {
                        jedis.set("node%%" + e.id, mapper.writeValueAsString(e));
                    } catch (JsonProcessingException e1) {
                        e1.printStackTrace();
                    }
                });
            }
        }
    }

    @Override
    public void saveBatch(List<Node> listEntity) throws SQLException {
        try (Connection connection = JdbcDataSource
                .getConnection(); PreparedStatement statement = connection.prepareStatement
                (insertNodeDdl)) {
            connection.setAutoCommit(false);
            for (Node entity : listEntity) {
                setNodeStatement(statement, entity);
                statement.addBatch();
            }
            if (listEntity.size() != 0) {
                statement.executeBatch();
                connection.commit();
            }
        }
    }

    @Override
    public void updateBatch(List<Node> listEntity) throws SQLException {
        try (Connection connection = JdbcDataSource
                .getConnection(); PreparedStatement statement = connection.prepareStatement
                (updateDdl)) {
            connection.setAutoCommit(false);
            List<Neighbour> neighbours = new LinkedList<>();
            listEntity.forEach(e -> neighbours.addAll(e.neighbours));
            edgeRepository.updateBatch(neighbours);
            for (Node node : listEntity) {
                setNodeStatement(statement, node);
                statement.setInt(10, node.id);
                statement.addBatch();
            }
            if (listEntity.size() != 0) {
                statement.executeBatch();
                connection.commit();
            }
        }
    }

    @Override
    public Node read(int id) throws SQLException {
        try (Connection connection = JdbcDataSource.getConnection();
             PreparedStatement ps = connection.prepareStatement(selectNodeDdl);
             PreparedStatement psEdge = connection.prepareStatement(selectEdgeDdl)) {
            ps.setInt(1, id);
            psEdge.setInt(1, id);
            ResultSet rset = psEdge.executeQuery();
            HashMap<Integer, Neighbour> map = new HashMap<>();
            while (rset.next()) {
                Neighbour neighbour = buildNeighbourFromResultSet(rset);
                map.put(neighbour.destination, neighbour);
            }
            ResultSet resultSet = ps.executeQuery();
            resultSet.next();
            return buildNodeFromResultSet(resultSet, map);

        }
    }

    private class TempNode {
        int id;
        int best_edge;
        int test_edge;
        int in_branch;
        int level;
        int find_count;
        byte state;
        Edge fragment_id;
        Edge best_weight;

        public TempNode(int id, int best_edge, int test_edge, int in_branch, int level, int find_count, byte state, Edge fragment_id, Edge best_weight) {
            this.id = id;
            this.best_edge = best_edge;
            this.test_edge = test_edge;
            this.in_branch = in_branch;
            this.level = level;
            this.find_count = find_count;
            this.state = state;
            this.fragment_id = fragment_id;
            this.best_weight = best_weight;
        }
    }

    private TempNode buildTempNodeFromResultSet(ResultSet resultSet) throws SQLException {
        int id = resultSet.getInt("id");
        int best_edge = resultSet.getInt("best_edge");
        int test_edge = resultSet.getInt("test_edge");
        int in_branch = resultSet.getInt("in_branch");
        int level = resultSet.getInt("level");
        int find_count = resultSet.getInt("find_count");
        byte state = resultSet.getByte("state");
        Edge fragment_id = Edge.buildFromString(resultSet.getString("fragment_id"));
        Edge best_weight = Edge.buildFromString(resultSet.getString("best_weight"));
        return new TempNode(id, best_edge, test_edge, in_branch, level, find_count, state, fragment_id, best_weight);
    }

    private Node buildNodeFromResultSet(ResultSet resultSet, HashMap<Integer, Neighbour> map)
            throws SQLException {
        TempNode tempNode = buildTempNodeFromResultSet(resultSet);
        return buildNodeFromTempNode(tempNode, map);
    }

    private Node buildNodeFromTempNode(TempNode tempNode, HashMap<Integer, Neighbour> map) {
        return new Node(tempNode.id, new LinkedList<>(map.values()), tempNode.state,
                map.getOrDefault(tempNode.best_edge, null),
                map.getOrDefault(tempNode.test_edge, null),
                map.getOrDefault(tempNode.in_branch, null), tempNode.best_weight, tempNode.find_count, tempNode.level,
                tempNode.fragment_id);
    }

    private Neighbour buildNeighbourFromResultSet(ResultSet resultSet) throws SQLException {
        int source = resultSet.getInt("source");
        int dest = resultSet.getInt("dest");
        double weight = resultSet.getDouble("weight");
        byte type = resultSet.getByte("type");
        return new Neighbour(source, dest, weight, type);
    }

    private void setNodeStatement(PreparedStatement statement, Node entity)
            throws SQLException {
        statement.setInt(1, entity.id);
        statement.setInt(2, entity.bestEdge != null ? entity.bestEdge.destination : -1);
        statement.setInt(3, entity.testEdge != null ? entity.testEdge.destination : -1);
        statement.setInt(4, entity.inBranch != null ? entity.inBranch.destination : -1);
        statement.setInt(5, entity.level);
        statement.setInt(6, entity.findCount);
        statement.setByte(7, entity.state);
        statement.setString(8, entity.fragmentId != null ? entity.fragmentId.serialized() : "");
        statement.setString(9, entity.bestWeight != null ? entity.bestWeight.serialized() : "");
    }
}
