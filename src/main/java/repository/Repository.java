package repository;


import java.sql.SQLException;
import java.util.List;

public interface Repository<T> {

  void save(T entity) throws SQLException;

  void update(T entity) throws SQLException;

  void saveBatch(List<T> listEntity) throws SQLException;

  void updateBatch(List<T> listEntity) throws SQLException;

  T read(int id) throws SQLException;
}
