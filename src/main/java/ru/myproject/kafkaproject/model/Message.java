package ru.myproject.kafkaproject.model;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;
import lombok.NoArgsConstructor;

public record Message(String topic, String key, String data) {
}



//@Data
//@NoArgsConstructor
//public class Message {
//
//    /**
//     * Kafka topic, в который отправляем/из которого читаем
//     */
//    @NotBlank
//    private String topic;
//
//    /**
//     * Опциональный ключ (нужен для партиционирования/упорядочивания)
//     */
//    private String key;
//
//    /**
//     * Полезная нагрузка
//     */
//    @NotBlank
//    private String data;
//}