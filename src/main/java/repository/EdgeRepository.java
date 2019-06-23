package repository;

import GHS.Neighbour;
import GHS.Node;

import java.sql.SQLException;
import java.util.List;

public class EdgeRepository implements Repository<Neighbour> {
    @Override
    public void save(Neighbour entity) throws SQLException {

    }

    @Override
    public void update(Neighbour entity) throws SQLException {

    }

    @Override
    public List<Node> loadTrivial(int first, int last) throws SQLException {
        return null;
    }
}
