package top.dalongm.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import top.dalongm.util.FlinkKafkaProducerCustom;

import java.util.Properties;

public class FlinkKafka2Kafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.1.130:9092");
        properties.setProperty("group.id", "test");

        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(
                "test",
                new SimpleStringSchema(),
                properties);
        // myConsumer.setStartFromGroupOffsets();
        myConsumer.setStartFromEarliest();
        DataStream<String> stream = env.addSource(myConsumer);
        stream.print();

        String topic = "test_out";

        FlinkKafkaProducer<String> myProducer = FlinkKafkaProducerCustom.create(topic, properties);

        myProducer.setWriteTimestampToKafka(true);

        stream.addSink(myProducer);

        env.execute();
    }
}
