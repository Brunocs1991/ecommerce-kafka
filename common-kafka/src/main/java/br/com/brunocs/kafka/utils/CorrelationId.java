package br.com.brunocs.kafka.utils;

import java.util.UUID;

public class CorrelationId {
    private final String id;

    public CorrelationId(String title) {
        this.id = String.format("%s(%s)", title, UUID.randomUUID());
    }


    @Override
    public String toString() {
        return "CorrelationId{" +
                "id='" + id + '\'' +
                '}';
    }

    public CorrelationId continueWith(String title) {
        return new CorrelationId(String.format("%s-%s", id, title));
    }
}
