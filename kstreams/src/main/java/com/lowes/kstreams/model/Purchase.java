package com.lowes.kstreams.model;

public class Purchase {
    private String customerId;
    private String productId;
    private int quantity;
    private long timestamp;

    public Purchase() {}

    public Purchase(String customerId, String productId, int quantity, long timestamp) {
        this.customerId = customerId;
        this.productId = productId;
        this.quantity = quantity;
        this.timestamp = timestamp;
    }

    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }

    public String getProductId() { return productId; }
    public void setProductId(String productId) { this.productId = productId; }

    public int getQuantity() { return quantity; }
    public void setQuantity(int quantity) { this.quantity = quantity; }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
}

