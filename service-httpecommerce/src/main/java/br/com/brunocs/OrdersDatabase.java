package br.com.brunocs;

import br.com.brunocs.database.LocalDatabase;
import br.com.brunocs.model.Order;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;

public class OrdersDatabase implements Closeable {

    private final LocalDatabase database;

    public OrdersDatabase() throws SQLException {
        this.database = new LocalDatabase("orders_database");
        this.database.createIfNotExists("CREATE TABLE Orders (" +
                "uuid varchar(200) primary key)");
    }

    public boolean saveNew(Order order) throws SQLException {
        if (wasProcessed(order)) {
            return false;
        }
        database.update("insert into Orders (uuid) values(?)", order.getOrderId());
        return true;
    }

    private boolean wasProcessed(Order order) throws SQLException {
        return database.query("select uuid from Orders where uuid = ? limit 1", order.getOrderId()).next();
    }


    @Override
    public void close() throws IOException {
        database.close();
    }
}
