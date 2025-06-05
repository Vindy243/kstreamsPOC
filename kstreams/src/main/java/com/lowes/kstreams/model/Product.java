package com.lowes.kstreams.model;

public class Product {
    private String productId;
    private double price;

    public Product() {}

    public Product(String productId, double price) {
        this.productId = productId;
        this.price = price;
    }

    public String getProductId() { return productId; }
    public void setProductId(String productId) { this.productId = productId; }

    public double getPrice() { return price; }
    public void setPrice(double price) { this.price = price; }
}

