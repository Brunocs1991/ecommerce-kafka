package br.com.brunocs;

import br.com.brunocs.kafka.consumer.ConsumerService;
import br.com.brunocs.kafka.consumer.ServiceRunner;
import br.com.brunocs.kafka.utils.Message;
import br.com.brunocs.model.User;
import br.com.brunocs.utils.IO;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

public class ReadingReportService implements ConsumerService<User> {

    private static final Path SOURCE = new File("src/main/resources/reports.txt").toPath();

    public static void main(String[] args) {
        new ServiceRunner<>(ReadingReportService::new).start(5);
    }

    @Override
    public void parse(ConsumerRecord<String, Message<User>> record) throws IOException {
        var message = record.value();
        System.out.println("---------------------");
        System.out.println("Processing report for" + record.value());
        var user = message.getPayload();
        var target = new File(user.getReportPath());
        IO.copyTo(SOURCE, target);
        IO.append(target, "Created for " + user.getUuid());

        System.out.println("File created: " + target.getAbsolutePath());

    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_USER_GENERATE_READING_REPORTER";
    }

    @Override
    public String getConsumerGroup() {
        return ReadingReportService.class.getSimpleName();
    }
}
