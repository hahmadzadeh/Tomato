package loader;


import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import utils.JdbcDataSource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class LoadGraphToPlatform {

    public int initialLoadFromTextFile(String filename) throws SQLException, IOException {
        String insertLeftEdge = "Insert Into tomato.\"Neighbour\" Select source, dest, 3, weight from tomato.\"edge_temp\" where source != dest";
        String insertRightEdge = "Insert Into tomato.\"Neighbour\" Select dest, source, 3, weight from tomato.\"edge_temp\" where source != dest";
        String insertNode = "Insert Into tomato.\"Node\" Select distinct(source) from tomato.\"Neighbour\" where source != dest";
        String deleteNeighbour = "Truncate tomato.\"Neighbour\"";
        String deleteNode = "Truncate tomato.\"Node\"";
        String deleteEdge = "Truncate tomato.\"edge_temp\"";
        String restartSeq = "ALTER SEQUENCE tomato.\"Node_iid_seq\" RESTART WITH 1;\n";
        String graphSize = "Select Count(*) from tomato.\"Node\"";
        try (Connection connection = JdbcDataSource.getConnection(); InputStream in = this.getClass().getResourceAsStream(filename)){
            connection.prepareStatement(deleteNeighbour).execute();
            connection.prepareStatement(deleteNode).execute();
            connection.prepareStatement(restartSeq).execute();
            connection.prepareStatement(deleteEdge).execute();
            new CopyManager((BaseConnection) connection.getMetaData().getConnection()).copyIn("Copy tomato.\"edge_temp\" from STDIN with delimiter \' \'",
                    new BufferedReader(new InputStreamReader(in)));
            connection.prepareStatement(insertLeftEdge).execute();
            connection.prepareStatement(insertRightEdge).execute();
            connection.prepareStatement(insertNode).execute();
            ResultSet resultSet = connection.prepareStatement(graphSize).executeQuery();
            resultSet.next();
            return resultSet.getInt(1);

        }
    }
}
