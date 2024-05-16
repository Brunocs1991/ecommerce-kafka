package br.com.brunocs;

import br.com.brunocs.kafka.KafkaService;
import br.com.brunocs.model.Order;
import br.com.brunocs.serializer.GsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

public class CreateUserService {

    private final Connection connection;

    public CreateUserService() throws SQLException {
        String url = "jdbc:sqlite:target/users_database.db";
        this.connection = java.sql.DriverManager.getConnection(url);
        try {
            connection.createStatement().execute("CREATE TABLE Users (" +
                    "uuid VARCHAR(200) PRIMARY KEY, " +
                    "email VARCHAR(200)" +
                    ")");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws SQLException {
        var createUserService = new CreateUserService();
        try (var service = new KafkaService(
                CreateUserService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                createUserService::parse,
                Order.class,
                Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName()))) {

            service.run();
        }
    }

    public void parse(ConsumerRecord<String, Order> record) throws SQLException {
        System.out.println("---------------------");
        System.out.println("Processing new order, checking for new user");
        System.out.println(record.value());
        var order = record.value();
        if (isNewUser(order.getEmail())) {
            insertNewUser(order.getEmail());

        }
    }

    private void insertNewUser(String email) throws SQLException {
        var insert = connection.prepareStatement("INSERT INTO Users (uuid, email) VALUES (?, ?) ");
        insert.setString(1, UUID.randomUUID().toString());
        insert.setString(2, email);
        insert.execute();
        System.out.println("User created: " + email);

    }

    private boolean isNewUser(String email) throws SQLException {
        var exists = connection.prepareStatement("SELECT uuid FROM Users WHERE email = ? limit 1 ");
        exists.setString(1, email);
        var results = exists.executeQuery();
        return !results.next();
    }

}
