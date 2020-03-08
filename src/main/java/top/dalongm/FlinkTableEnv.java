package top.dalongm;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Properties;

public class FlinkTableEnv {
    public static void main(String[] args) {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.1.130:9092");
        properties.setProperty("group.id", "test");
        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(
                "test",
                new SimpleStringSchema(),
                properties);
        // myConsumer.setStartFromGroupOffsets();
        myConsumer.setStartFromEarliest();
        DataStream<String> stream = fsEnv.addSource(myConsumer);

        Table table = fsTableEnv.fromDataStream(stream);

    }
}
