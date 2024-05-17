package br.com.brunocs;

import br.com.brunocs.kafka.consumer.ConsumerService;
import br.com.brunocs.kafka.consumer.ServiceRunner;
import br.com.brunocs.kafka.utils.Message;
import br.com.brunocs.model.Email;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmailService implements ConsumerService<Email> {

    private static final Logger logger = LoggerFactory.getLogger(EmailService.class);

    public static void main(String[] args) {
        new ServiceRunner<>(EmailService::new).start(5);
    }

    public String getConsumerGroup() {
        return EmailService.class.getSimpleName();
    }

    public String getTopic() {
        return "ECOMMERCE_SEND_EMAIL";
    }

    public void parse(ConsumerRecord<String, Message<Email>> record) {
        System.out.println("---------------------");
        System.out.println("sending email");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
        System.out.println("Email Send");
    }
}
