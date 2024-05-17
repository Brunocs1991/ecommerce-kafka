package br.com.brunocs;

import br.com.brunocs.kafka.KafkaService;
import br.com.brunocs.kafka.Message;
import br.com.brunocs.model.User;
import br.com.brunocs.utils.IO;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

public class ReadingReportService {

    private static final Path SOURCE = new File("src/main/resources/reports.txt").toPath();

    public static void main(String[] args) {
        var readingReportService = new ReadingReportService();
        try (var service = new KafkaService<>(ReadingReportService.class.getSimpleName(),
                "ECOMMERCE_USER_GENERATE_READING_REPORTER",
                readingReportService::parse,
                Map.of())) {
            service.run();
        }

    }

    private void parse(ConsumerRecord<String, Message<User>> record) throws IOException {
        var message = record.value();
        System.out.println("---------------------");
        System.out.println("Processing report for" + record.value());
        var user = message.getPayload();
        var target = new File(user.getReportPath());
        IO.copyTo(SOURCE, target);
        IO.append(target, "Created for " + user.getUuid());

        System.out.println("File created: " + target.getAbsolutePath());

    }
}
