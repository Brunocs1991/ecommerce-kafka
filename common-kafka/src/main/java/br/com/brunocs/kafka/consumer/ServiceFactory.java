package br.com.brunocs.kafka.consumer;

public interface ServiceFactory<T> {
    ConsumerService<T> create();
}
