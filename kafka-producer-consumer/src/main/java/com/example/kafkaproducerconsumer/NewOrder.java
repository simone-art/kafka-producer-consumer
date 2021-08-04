package com.example.kafkaproducerconsumer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;

public class NewOrder {

    //Kafka precisa de propiedades para definir onde será a conexao
    //Mensagem que o producer vai enviar tem que ser gravada; Por isso se usa o record
    //Producer record usa também os parámetros de chave e valor
    // Na variável record vai ser definido o topic
    public static void main(String[] args) {
        var producer = new KafkaProducer<String, String>(properties());
        var value = "12345, 76523, 300";
        var record = new ProducerRecord<String, String>("ECOMMERCE_NEW_ORDER", value, value );
        producer.send(record);
    }

    //criado método estático
    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }
}
