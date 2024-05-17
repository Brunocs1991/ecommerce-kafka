package br.com.brunocs;

import br.com.brunocs.kafka.CorrelationId;
import br.com.brunocs.kafka.KafkaDispatch;
import br.com.brunocs.kafka.KafkaService;
import br.com.brunocs.kafka.Message;
import br.com.brunocs.model.User;
import br.com.brunocs.serializer.GsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class BatchSendMessageService {
    private final Connection connection;
    private final KafkaDispatch<User> userDispatch = new KafkaDispatch<>();

    public BatchSendMessageService() throws SQLException {
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
        var batchService = new BatchSendMessageService();
        try (var service = new KafkaService(
                BatchSendMessageService.class.getSimpleName(),
                "ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS",
                batchService::parse,
                Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName()))) {

            service.run();
        }
    }

    public void parse(ConsumerRecord<String, Message<String>> record) throws ExecutionException, InterruptedException, SQLException {
        var message = record.value();
        System.out.println("---------------------");
        System.out.println("Processing new batch");
        System.out.println("Topic: " + message.getPayload());
        for (User user : getAllUsers()) {
            userDispatch.send(
                    message.getPayload(),
                    user.getUuid(),
                    message.getId().continueWith(BatchSendMessageService.class.getSimpleName()),
                    user);
        }
    }

    private List<User> getAllUsers() throws SQLException {
        var results = connection.prepareStatement("SELECT uuid FROM Users").executeQuery();
        List<User> users = new ArrayList<>();
        while (results.next()) {
            users.add(new User(results.getString("uuid")));
        }
        System.out.println(users.size());
        return users;
    }
}
