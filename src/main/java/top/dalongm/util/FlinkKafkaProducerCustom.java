package top.dalongm.util;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class FlinkKafkaProducerCustom {

    public static FlinkKafkaProducer<String> create(String topic, Properties properties) {
        return new FlinkKafkaProducer<>(topic,
                (KafkaSerializationSchema<String>) (element, timestamp) -> new ProducerRecord<>(topic, element.getBytes()),
                properties,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
    }
}
