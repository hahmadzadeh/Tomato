package repository;

import GHS.Node;

import java.sql.SQLException;
import java.util.List;

public interface Repository<T> {
    void save(T entity) throws SQLException;
    void update(T entity) throws SQLException;
    List<Node> loadTrivial(int first, int last) throws SQLException;
    void saveBatch(List<T> listEntity) throws  SQLException;
}
