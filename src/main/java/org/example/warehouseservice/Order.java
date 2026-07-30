package org.example.warehouseservice;

public record Order(
        String orderID,
        String product,
        Integer quantity
) {
}
