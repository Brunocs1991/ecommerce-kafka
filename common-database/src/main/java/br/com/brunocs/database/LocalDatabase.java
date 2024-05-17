package br.com.brunocs.database;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Hello world!
 */
public class LocalDatabase {


    private final Connection connection;

    public LocalDatabase(String name) throws SQLException {
        String url = String.format("jdbc:sqlite:target/%s.db", name);
        this.connection = java.sql.DriverManager.getConnection(url);
    }

    public void createIfNotExists(String sql) throws SQLException {
        try {
            connection.createStatement().execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void update(String statement, String... params) throws SQLException {
        prepare(statement, params).executeUpdate();
    }

    public ResultSet query(String query, String... params) throws SQLException {
        return prepare(query, params).executeQuery();
    }

    private PreparedStatement prepare(String statement, String[] params) throws SQLException {
        var preparedStatement = connection.prepareStatement(statement);
        for (int i = 0; i < params.length; i++) {
            preparedStatement.setString(i + 1, params[i]);
        }
        return preparedStatement;
    }

    public void close() throws IOException {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }
}
