package br.com.brunocs.model;

import java.math.BigDecimal;

public class Order {

    private final String orderId;
    private final String email;
    private final BigDecimal amount;

    public Order(String orderId, String email, BigDecimal amount) {
        this.orderId = orderId;
        this.email = email;
        this.amount = amount;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public String getEmail() {
        return email;
    }

    @Override
    public String toString() {
        return "Order{" +
                "orderId='" + orderId + '\'' +
                ", email='" + email + '\'' +
                ", amount=" + amount +
                '}';
    }
}
