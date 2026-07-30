package org.example.warehouseservice;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

@Service
public class OrderKafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderKafkaConsumer.class);

    private final ObjectMapper objectMapper;

    public OrderKafkaConsumer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "orders", groupId = "warehouse-group")
    public void listenOrders(String orderJson) {
        log.info("Received raw message from Kafka: {}", orderJson);

        try {
            Order order = objectMapper.readValue(orderJson, Order.class);

            log.info("Successfully parsed order: ID={}, Product={}, Quantity={}",
                    order.orderID(), order.product(), order.quantity());

            processWarehouseLogic(order);

        } catch (Exception e) {
            log.error("Error parsing or processing order. Raw data: {}", orderJson, e);
        }
    }

    private void processWarehouseLogic(Order order) {
        log.info("Warehouse started processing order #{}", order.orderID());
    }
}