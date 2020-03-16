package top.dalongm;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import top.dalongm.entity.User;

public class FlinkKafka2Redis {
    public static void main(String[] args) throws Exception {
        String host = "192.168.1.130";
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);


        fsTableEnv
                // declare the external system to connect to
                // https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/connect.html#kafka-connector
                .connect(
                        new Kafka()
                                .version("universal")
                                .topic("test")
                                .startFromLatest()
                                .property("zookeeper.connect", host + ":2181")
                                .property("bootstrap.servers", host + ":9092")
                                .property("group.id", "flink-test")
                )

                // declare a format for this system
                // https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/connect.html#json-format
                .withFormat(
                        new Json().failOnMissingField(true)   // optional: flag whether to fail if a field is missing or not, false by default
                )

                // declare the schema of the table
                .withSchema(
                        new Schema()
                                .field("user", DataTypes.STRING())
                                .field("message", DataTypes.STRING())
                                .field("timestamp", DataTypes.STRING())
                )
                .inAppendMode()

                // create a table with given name
                .createTemporaryTable("MyUserTable");

        Table user = fsTableEnv.from("MyUserTable");
        user.printSchema();


        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost(host).setDatabase(0).setPort(6379).build();

        DataStream<User> stream = fsTableEnv.toAppendStream(user.select("user,message,timestamp"), User.class);

        stream.addSink(new RedisSink<User>(conf, new RedisExampleMapper()));

        fsEnv.execute();
    }

    /**
     * http://bahir.apache.org/docs/flink/current/flink-streaming-redis/
     */
    public static class RedisExampleMapper implements RedisMapper<User> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.SET, "KEY_NAME");
        }

        @Override
        public String getKeyFromData(User data) {
            return data.getUser();
        }

        @Override
        public String getValueFromData(User data) {
            return data.getMessage();
        }
    }
}
