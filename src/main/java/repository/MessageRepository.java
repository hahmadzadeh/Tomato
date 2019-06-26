package repository;

import GHS.Message;
import java.sql.SQLException;
import java.util.List;

public class MessageRepository implements Repository<Message> {

    @Override
    public void save(Message entity) throws SQLException {

    }

    @Override
    public void update(Message entity) throws SQLException {

    }

    @Override
    public void saveBatch(List<Message> listEntity) throws SQLException {

    }

    @Override
    public void updateBatch(List<Message> listEntity) throws SQLException {

    }

    @Override
    public Message read(int id) throws SQLException {
        return null;
    }
}
