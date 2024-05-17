package br.com.brunocs;

import br.com.brunocs.database.LocalDatabase;
import br.com.brunocs.kafka.consumer.ConsumerService;
import br.com.brunocs.kafka.consumer.ServiceRunner;
import br.com.brunocs.kafka.utils.Message;
import br.com.brunocs.model.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.SQLException;
import java.util.UUID;

public class CreateUserService implements ConsumerService<Order> {
    private final LocalDatabase database;

    public CreateUserService() throws SQLException {
        this.database = new LocalDatabase("users_database");
        this.database.createIfNotExists("CREATE TABLE Users (" +
                "uuid VARCHAR(200) PRIMARY KEY, " +
                "email VARCHAR(200)" +
                ")");
    }

    public static void main(String[] args) {

        new ServiceRunner<>(CreateUserService::new).start(1);

    }

    @Override
    public void parse(ConsumerRecord<String, Message<Order>> record) throws SQLException {
        var order = record.value().getPayload();
        System.out.println("---------------------");
        System.out.println("Processing new order, checking for new user");
        System.out.println(record.value());
        if (isNewUser(order.getEmail())) {
            insertNewUser(order.getEmail());

        }
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return CreateUserService.class.getSimpleName();
    }

    private void insertNewUser(String email) throws SQLException {

        database.update("INSERT INTO Users (uuid, email) VALUES (?, ?) ",  UUID.randomUUID().toString(), email);
        System.out.println("User created: " + email);

    }

    private boolean isNewUser(String email) throws SQLException {
        var results = database.query("SELECT uuid FROM Users WHERE email = ? limit 1 ", email);

        return !results.next();
    }

}
