package repository;

import GHS.Neighbour;
import GHS.Node;
import utils.JdbcDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class EdgeRepository implements Repository<Neighbour> {

    private final static String insertEdgeDDL = "INSERT INTO tomato.\"Neighbour\"(\n" +
        "\tsource, dest, type, weight)\n" +
        "\tVALUES (?, ?, ?, ?)";
    private final static String updateEdgeDDL = "UPDATE tomato.\"Neighbour\"\n" +
        "\tSET source=?, dest=?, type=?, weight=?\n" +
        "\tWHERE source=?";

    @Override
    public void save(Neighbour entity) throws SQLException {
        try (Connection connection = JdbcDataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement(insertEdgeDDL)) {
            setEdgeStatement(entity, statement);
            statement.execute();
        }
    }

    private void setEdgeStatement(Neighbour entity, PreparedStatement statement)
        throws SQLException {
        statement.setInt(1, entity.source);
        statement.setInt(2, entity.destination);
        statement.setByte(3, entity.type);
        statement.setDouble(4, entity.edge.weight);
    }

    @Override
    public void update(Neighbour entity) throws SQLException {
        try (Connection connection = JdbcDataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement(updateEdgeDDL)) {
            setEdgeStatement(entity, statement);
            statement.setInt(5, entity.source);
            statement.execute();
        }
    }

    @Override
    public void saveBatch(List<Neighbour> listEntity) throws SQLException {
        try (Connection connection = JdbcDataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement(insertEdgeDDL)) {
            for(Neighbour entity: listEntity){
                setEdgeStatement(entity, statement);
                statement.addBatch();
            }
            statement.execute();
        }
    }
}
