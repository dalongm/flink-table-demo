package top.dalongm.sink;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * *{
 * *    "timestamp": "1583662978",
 * *    "user": 11111,
 * *    "message": "bye,world!"
 * *}
 */
public class FlinkKafka2MySQL {
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
                        new Json().failOnMissingField(false)   // optional: flag whether to fail if a field is missing or not, false by default
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

        // TupleTypeInfo<Tuple2<String, String>> tupleType = new TupleTypeInfo<>(Types.STRING(), Types.STRING());
        // DataStream<User> dsRow = fsTableEnv.toAppendStream(user.select("user,message,timestamp"), User.class);
        // dsRow.print();


        // https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/connect.html#jdbcappendtablesink
        JDBCAppendTableSink sink = JDBCAppendTableSink.builder()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://localhost:3306/user?useSSL=false&serverTimezone=UTC")
                .setUsername("root")
                .setPassword("root")
                .setQuery("INSERT INTO user (user,message,timestamp) VALUES (?,?,?)")
                .setParameterTypes(new TypeInformation[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO})
                .build();

        // 方式1
        // fsTableEnv.registerTableSink(
        //         "jdbcOutputTable",
        //         // specify table schema
        //         new String[]{"user", "message", "timestamp"},
        //         new TypeInformation[]{Types.STRING(), Types.STRING(), Types.STRING()},
        //         sink);
        //
        // user.select("user,message,timestamp").insertInto("jdbcOutputTable");

        // 方式2
        DataStream<Row> stream = fsTableEnv.toAppendStream(user, Row.class);
        sink.emitDataStream(stream);


        fsEnv.execute();
    }
}
