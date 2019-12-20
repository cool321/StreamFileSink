import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author lijiku
 * @date 2019-09-12
 */
public class Main {

    public static void main(String[] args) throws Exception {
        Boolean first =true;
        String topicStr = "flink20";
        List<String> topics = Arrays.asList(topicStr.split(","));

        //01 设置flink运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.setStateBackend(new RocksDBStateBackend("hdfs:///user/flink/checkpoints/"));
        env.enableCheckpointing(30000);   //设置checkpoint的相关参数

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime); //设置时间属性为ProcessingTime
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 1000));

        //02 设置消费kafka的相关参数，获取sourceStream

        Properties props = new Properties();

        String kafka_address= "localhost:9092";
        String group = "test";
        props.setProperty("bootstrap.servers", kafka_address);
        props.put("enable.auto.commit", "false");
        props.setProperty("group.id", group);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        FlinkKafkaConsumer011<String> Consumer = new FlinkKafkaConsumer011<>(topics, new SimpleStringSchema(), props);
        Consumer.setCommitOffsetsOnCheckpoints(true);
        if (first) {
            Consumer.setStartFromEarliest();
        }

        DataStreamSource<String> stream = env.addSource(Consumer);
//
//        DataStreamSource<String> stream = env.socketTextStream("localhost", 7780);

        RollingPolicy<String, String> rollingPolicy = DefaultRollingPolicy.create()
                .withRolloverInterval(10000)
                .build();
        StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path("hdfs:///user/tal/tmp"), new SimpleStringEncoder<String>())
                .withBucketAssigner(new EventTimeBucketAssigner())
                .withRollingPolicy(rollingPolicy)
                .withBucketCheckInterval(5000)
                .build();
        stream.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                Random random = new Random();
                int i = random.nextInt(2);
                if(value.equalsIgnoreCase("a")  ){
                    throw  new RuntimeException("as");
                }
                return value;
            }

        }).addSink(sink);




        env.execute("flink_kafka_");


    }


    public static  class EventTimeBucketAssigner implements BucketAssigner<String, String> {

        @Override
        public String getBucketId(String element, Context context) {
            String partitionValue;
            try {
                partitionValue = getPartitionValue(element);
            } catch (Exception e) {
                partitionValue = "00000000";
            }
            return "dt=" + partitionValue;
        }

        @Override
        public SimpleVersionedSerializer<String> getSerializer() {
            return SimpleVersionedStringSerializer.INSTANCE;
        }

        private String getPartitionValue(String element) throws Exception {

            return new SimpleDateFormat("yyyyMMdd").format(new Date());
        }
    }

}


