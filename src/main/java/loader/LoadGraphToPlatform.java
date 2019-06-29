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

    public int initialLoadFromTextFile(String filename, boolean oneSide) throws SQLException, IOException {
        String insertLeftEdge = "Insert Into tomato.\"Neighbour\" Select source, dest, 3, weight from tomato.\"edge_temp\" where source != dest";
        String insertRightEdge = "Insert Into tomato.\"Neighbour\" Select dest, source, 3, weight from tomato.\"edge_temp\" where source != dest";
        String insertNode = "Insert Into tomato.\"Node\" Select distinct(source) from tomato.\"Neighbour\" where source != dest";
        String deleteNeighbour = "Truncate tomato.\"Neighbour\"";
        String deleteNeighbour2 = "Truncate tomato.\"Neighbour2\"";
        String deleteNode = "Truncate tomato.\"Node\"";
        String deleteNode2 = "Truncate tomato.\"Node2\"";
        String deleteEdge = "Truncate tomato.\"edge_temp\"";
        String restartSeq = "ALTER SEQUENCE tomato.\"Node_iid_seq\" RESTART WITH 1;\n";
        String restartSeq2 = "ALTER SEQUENCE tomato.\"Node2_iid_seq\" RESTART WITH 1;\n";
        String graphSize = "Select Count(*) from tomato.\"Node\"";
        try (Connection connection = JdbcDataSource.getConnection(); InputStream in = this.getClass().getResourceAsStream(filename)) {
            connection.prepareStatement(deleteNeighbour).execute();
            connection.prepareStatement(deleteNode).execute();
            connection.prepareStatement(restartSeq).execute();
            connection.prepareStatement(deleteEdge).execute();
            connection.prepareStatement(deleteNeighbour2).execute();
            connection.prepareStatement(deleteNode2).execute();
            connection.prepareStatement(restartSeq2).execute();
            new CopyManager((BaseConnection) connection.getMetaData().getConnection()).copyIn("Copy tomato.\"edge_temp\" from STDIN with delimiter \' \'",
                    new BufferedReader(new InputStreamReader(in)));
            connection.prepareStatement(insertLeftEdge).execute();
            if (!oneSide)
                connection.prepareStatement(insertRightEdge).execute();
            connection.prepareStatement(insertNode).execute();
            ResultSet resultSet = connection.prepareStatement(graphSize).executeQuery();
            resultSet.next();
            return resultSet.getInt(1);

        }
    }
}
