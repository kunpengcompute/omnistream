# User Guide<a name="ZH-CN_TOPIC_0000002549640817"></a>

## Prerequisites

Required software has been installed. For details about how to install the software, see [Installation Guide](./installation_guide.md).

## Using the Feature<a name="ZH-CN_TOPIC_0000002549520803"></a>

### Supported Operators and Expressions<a name="ZH-CN_TOPIC_0000002549640813"></a>

This section describes the scope, restrictions, and usage rules of SQL operators and expressions (including data types) supported by the OmniStream Flink Native feature in Flink versions 1.16.3, 1.17.1, and 1.20.0.

[**Table 2** Supported operators](#supported-operators) and [**Table 3** Supported expressions](#supported-expressions) list the operators, expressions, and functions supported by OmniStream. [**Table 1** Meanings of the symbols](#meanings-of-the-symbols) lists the symbols indicating whether the operators and expressions are supported.

>![](public_sys-resources/icon-notice.gif) **NOTICE**
>
>- [**Table 2** Supported operators](#supported-operators) and [**Table 3** supported-expressions](#supported-expressions) list only the data types supported or involved by OmniStream. Other data types are not supported by OmniStream.
>- If you use operators and expressions that are not supported by OmniStream, the execution plan will be rolled back to native execution, which deteriorates the performance.
>- When you use the SQL Client interactive user interface to execute SQL statements, you are advised to export the execution result to the data table whose connector is **blackhole**. For details, see the execution mode of Nexmark Q0.
>- Due to memory restrictions, only the Calc and LookupJoin operators are supported by default. To support other operators, set export `export FLINK\_PERFORMANCE` to `false` to enable environment variables.

**Table 1** Meanings of the symbols<a id="meanings-of-the-symbols"></a>

|Symbol| Meaning                                                               |
|--|-------------------------------------------------------------------|
|S| Indicates that the operator or expression is supported.                                                     |
|PS| Indicates that the operator or expression is partially supported, with some restrictions. For details, see [Constraints](../../README_en.md#constraints).|
|NS| Indicates that the operator or expression is not supported.                                                    |
|NA| Indicates that the operator or expression is not involved. This scenario does not exist in open-source Flink.                                  |
|[Blank Cell]| Indicates a scenario that is irrelevant or needs to be confirmed.                                                      |

**Table 2** Supported operators<a id="supported-operators"></a>

|Operator|BIGINT|VARCHAR|TIMESTAMP(3)|
|--|--|--|--|
|Calc|S|S|S|
|Sink|S|S|S|
|Csv Source|S|S|S|
|Kafka Source|S|S|S|
|Kafka Sink|S|S|S|
|Join|PS|PS|PS|
|LookupJoin|PS|PS|PS|
|GroupAggregate|PS|PS|PS|
|LocalGroupAggregate|PS|PS|PS|
|GlobalGroupAggregate|PS|PS|PS|
|IncrementalGroupAggregate|PS|PS|PS|
|LocalWindowAggregate|PS|PS|PS|
|GlobalWindowAggregate|PS|PS|PS|
|GroupWindowAggregate|PS|PS|PS|
|WindowAggregate|PS|PS|PS|
|WindowJoin|PS|PS|PS|
|Deduplicate|PS|PS|PS|
|Expand|PS|PS|PS|
|Rank|PS|PS|PS|

**Table 3** Supported expressions<a id="supported-expressions"></a>

|Expression|Function Type|BIGINT|VARCHAR|NULL|TIMESTAMP(3)|
|--|--|--|--|--|--|
|*|Scalar Functions|S|NS|S|S|
|+|Scalar Functions|S|NS|S|S|
|-|Scalar Functions|S|NS|S|S|
|/|Scalar Functions|S|NS|S|S|
|LOWER|Scalar Functions|NA|S|NA|NA|
|SPLIT_INDEX|Scalar Functions|S|S|NA|NA|
|DATE_FORMAT|Scalar Functions|NA|NA|NA|S|
|COUNT_CHAR|Scalar Functions|NA|S|NA|NA|
|HOUR|Scalar Functions|S|NA|NS|S|
|REGEX_EXTRACT|Scalar Functions|NA|S|NS|NA|
| JSON_VALUE | Scalar Functions | NA | S | S | NA |
| JSON_QUERY | Scalar Functions | NA | S | S | NA |
| COALESCE | Scalar Functions | S | S | S | S |
| PROCTIME_MATERIALIZE | Scalar Functions | NA | NA | NA | S |
| CHAR_LENGTH | Scalar Functions | NA | S | NA | NA |
| TO_TIMESTAMP_LTZ | Scalar Functions | S | NA | S | NA |

### Supported DataStream Operators and UDFs<a name="ZH-CN_TOPIC_0000002517961054"></a>

This section describes the scope of support, restrictions, and performance impact of the OmniStream Flink Native feature on DataStream operators and user-defined functions (UDFs) in Flink 1.16.3.

>![](public_sys-resources/icon-notice.gif) **NOTICE**
>If you use DataStream operators and UDFs that are not supported by OmniStream, the execution plan will be rolled back to native execution, which deteriorates the performance.

- The DataStream operators supported by OmniStream include Kafka Source, Kafka Sink, Map, Reduce, FlatMap, and Filter.
- The UDF trustlist is provided from multiple dimensions, including data transfer objects, function types, UDF dependency classes and interfaces, Java type translation, and Java statement translation. For details, see [Trustlisted UDFs](#section92601228172312).

**Trustlisted UDFs<a name="section92601228172312"></a>**

The supported data transfer objects include Long, String, and Tuple2<String, Long\>.

[**Table  1** Supported expressions](#supported-expressions-1) lists the supported dependency classes and interfaces. For details about other constraints, see [UDF Translator User Guide](https://gitcode.com/openeuler/docs/blob/stable-24.03_LTS_SP2/docs/en/server/development/unt/unt_guide.md). The supported expressions may vary depending on the environment configuration. If there are differences, you can submit an issue to report the problem.

**Table 1** Supported expressions<a id="supported-expressions-1"></a>

|Java Class|Java Class Interface|
|--|--|
|Arrays|static \<T> List\<T> asList(Array);|
|HashMap (The hashCode and equals methods must be implemented for all accessed elements.)|Object get(Object key);<br> Object put(Object key, Object value);<br> void putAll(HashMap m);<br> boolean containsKey(Object key);<br> int size();<br> boolean remove (Object key) (Different from Java interfaces, variables cannot be used to carry return values.);<br> Set<Map.Entry<Object,Object>> entrySet();<br> Set\<Object> keySet();<br> HashMap clone();|
|Iterator|boolean hasNext();<br> Object next();|
|ArrayList|Object get(int index);<br> void clear();<br> void add(Object e);<br> Iterator iterator();<br> boolean contains(Object o);<br> int size();<br> boolean isEmpty()|
|LinkedList|Object getFirst();<br> Object getLast();<br> void addLast(Object e);<br> void addFirst(Object e);<br>|
|Map.Entry (The hash and equals methods must be implemented for elements in mapentry.)|Object getKey();<br> Object getValue();<br> void setValue(Object value); (Different from Java interfaces, variables cannot be used to carry return values.)|
|HashSet (The hash and equals methods must be implemented for accessed elements.)|boolean addAll(ArrayList list);<br> boolean add(Object e);<br> boolean remove(Object o);<br> boolean contains(Object o);<br> int size();<br> void clear();<br> Iterator iterator();<br>|
|StringBuilder|StringBuilder append(String str);<br> String toString();<br>|
|Array (Only one-dimensional arrays of the object type are supported. Basic arrays and multi-dimensional arrays are not supported.)|Size;<br> Get elements;<br> Put elements (in sequence);|
|Integer|String toString();<br> bool equals(Integer *obj);<br> overrideint intValue();<br> static Integer valueOf(String s);<br> static Integer valueOf(int i);|
|Boolean|static Boolean valueOf;<br> (boolean b)boolean booleanValue()|
|Long|int hashCode();<br> boolean equals(Long obj);<br> String toString();<br> Long clone();<br> long longValue();<br> static Long valueOf(String s);<br> static Long valueOf(long l);<br>|
|Object|int hashCode();<br> bool equals(Object *obj);<br> String toString();<br> Object clone();<br>|
|String|int hashCode();<br> boolean equals(String anObject);<br> String toString();<br> Object clone();<br> String replace(String target, String replacement);<br>  String[] split(String regex); (Character strings can be split. Regular expressions are not supported.)<br> String replaceAll(String regex, String replacement);<br> int lastIndexOf(String str);<br> int length();<br> String substring(int beginIndex);<br> String substring(int beginIndex, int endIndex);<br> boolean contains(String s);<br> boolean endsWith(String suffix);<br> boolean startsWith(String prefix);<br>|
|Gson|String toJson(HashMap<String,String> map);<br> Map fromJson(String json, Type typeOf); (Only the String to Map type conversion is supported.)|
|JsonObject|JsonObject getAsJsonObject(String memberName); (Only String constants are supported.)|
|JsonParser|static JsonObject parseString(String json);|
|JsonPrimitive|boolean getAsBoolean();|
|JsonElement|JsonObject getAsJsonObject();<br> double getAsDouble();<br> float getAsFloat();<br> int getAsInt();<br> long getAsLong();<br> short getAsShort();<br> boolean getAsBoolean();<br> String getAsString();<br> boolean isJsonNull();<br> String toString();<br> String toString();<br>|
|JsonArray|Iterator\<JsonElement> iterator();<br>|

### (SQL Scenario) Enabling OmniStream<a name="ZH-CN_TOPIC_0000002549640821"></a>

This section describes how to start a Flink cluster and enable OmniStream in SQL scenarios.

1. Access the `flink\_jm\_8c32g` container and start the Flink cluster.

    ```bash
    docker exec -it flink_jm_8c32g /bin/bash
    source /etc/profile
    cd /usr/local/flink-1.16.3/bin
    ./start-cluster.sh
    ```

    >![](public_sys-resources/icon-notice.gif) **NOTICE**
    >Each time you exit and access the container again, you need to run the `source /etc/profile` command to reload the environment variables. This ensures that the dependencies are properly detected when running tasks.

2. Check whether the Job Manager and Task Manager are started successfully.
    1. Check the `flink\_jm\_8c32g` container for the `StandaloneSessionClusterEntrypoint` process.

        ```bash
        source /etc/profile
        jps
        ```

        If the `StandaloneSessionClusterEntrypoint` process exists, the Job Manager is started successfully.

        ![](figures/en-us_image_0000002517961062.png)

    2. Access the `flink\_tm1\_8c32g` and `flink\_tm2\_8c32g` containers and check whether the `TaskManagerRunner` process exists. The following commands use the `flink\_tm1\_8c32g` container as an example:

        ```bash
        docker exec -it flink_tm1_8c32g /bin/bash
        source /etc/profile
        jps
        ```

        If the `TaskManagerRunner` process exists, the Task Manager is started successfully.

        ![](figures/en-us_image_0000002517961060.png)

3. Start Nexmark in the `flink\_jm\_8c32g` container.

    ```bash
    docker exec -it flink_jm_8c32g /bin/bash
    source /etc/profile
    cd /usr/local/nexmark/bin
    ./setup_cluster.sh
    ```

4. Access the `flink\_tm1\_8c32g` and `flink\_tm2\_8c32g` containers and check whether Nexmark has been started successfully. The following commands use the `flink\_tm1\_8c32g` container as an example:

    ```bash
    docker exec -it flink_tm1_8c32g /bin/bash
    source /etc/profile
    jps
    exit
    ```

    If the `CpuMetricSender` process exists, Nexmark has been started successfully.

    ![](figures/en-us_image_0000002549640829.png)

5. Execute the Nexmark test case `Query0` in the `flink\_jm\_8c32g` container.

    ```bash
    docker exec -it flink_jm_8c32g /bin/bash
    source /etc/profile
    cd /usr/local/nexmark/bin
    sh run_query.sh q0
    exit
    ```

    View the execution result. The expected result is that the test case runs successfully without any errors.

    ![](figures/en-us_image_0000002549640827.png)

6. View the latest `.out` log file of Flink in the container that hosts the Task Manager.

    ```bash
    docker exec -it flink_tm1_8c32g /bin/bash
    cd /usr/local/flink-1.16.3/log
    ```

    - If the log contains "Shared Memory Metric Manager Loading Succeed", the native SO library has been loaded.
    - If the log contains "welcome to native", OmniStream has been enabled successfully.

    ![](figures/en-us_image_0000002517961058.png)

### (DataStream Scenario) Enabling OmniStream<a name="ZH-CN_TOPIC_0000002518120974"></a>

This section describes how to start a Flink cluster and enable OmniStream in DataStream scenarios.

1. If DataStream tasks are running in a multi-Task-Manager environment, add `omni.batch: true` to the `flink-conf.yaml` file to improve shuffle efficiency and achieve better performance.
    1. Access the `flink_jm_8c32g` container and add `omni.batch: true` to the `flink-conf.yaml` file.

        ```bash
        docker exec -it flink_jm_8c32g /bin/bash
        ```

    2. Open the `/usr/local/flink/conf/flink-conf.yaml` file.

         ```bash
        vi /usr/local/flink/conf/flink-conf.yaml
         ```

    3. Press `i` to enter the insert mode and add the following content to the file:

         ```bash
        omni.batch: true
         ```

    4. Press `Esc`, type `:wq!`, and press `Enter` to save the file and exit.
    5. Access the `flink_tm1_8c32g` and `flink_tm2_8c32g` containers in sequence and add `omni.batch: true` to the `flink-conf.yaml` file.

         ```bash
        docker exec -it flink_tm1_8c32g /bin/bash
        vi /usr/local/flink/conf/flink-conf.yaml
        omni.batch: true
        
        docker exec -it flink_tm2_8c32g /bin/bash
        vi /usr/local/flink/conf/flink-conf.yaml
        omni.batch: true
         ```

2. Access the `flink\_jm\_8c32g` container and start the Flink cluster.

    ```bash
    docker exec -it flink_jm_8c32g /bin/bash
    source /etc/profile
    cd /usr/local/flink-1.16.3/bin
    ./start-cluster.sh
    ```

    >![](public_sys-resources/icon-notice.gif) **NOTICE**
    > Each time you exit and access the container again, you need to run the `source /etc/profile` command to reload the environment variables. This ensures that the dependencies are properly detected when running tasks.

3. Check whether the Job Manager and Task Manager are started successfully.
    1. Check the `flink\_jm\_8c32g` container for the `StandaloneSessionClusterEntrypoint` process.

        ```bash
        source /etc/profile
        jps
        ```

        If the `StandaloneSessionClusterEntrypoint` process exists, the Job Manager is started successfully.

        ![](figures/en-us_image_0000002549520817.png)

    2. Access the `flink\_tm1\_8c32g` and `flink\_tm2\_8c32g` containers and check whether the `TaskManagerRunner` process exists. The following commands use the `flink\_tm1\_8c32g` container as an example:

        ```bash
        docker exec -it flink_tm1_8c32g /bin/bash
        source /etc/profile
        jps
        ```

        If the `TaskManagerRunner` process exists, the Task Manager is started successfully.

        ![](figures/en-us_image_0000002518120982.png)

4. Create and configure the Kafka consumer and producer configuration files.
    1. Access the `flink_tm1_8c32g` container.

        ```bash
        docker exec -it flink_tm1_8c32g /bin/bash
        ```

    2. Create the `/opt/conf` directory. <a id="4.2"></a>

        ```bash
        mkdir /opt/conf
        cd /opt/conf
        ```

    3. Create the Kafka consumer configuration file `kafka\_consumer.conf`.

        ```bash
        fetch.queue.backoff.ms=20
        group.id=omni
        max.poll.records=10000
        ```

    4. Create the Kafka producer configuration file `kafka_producer.conf`.<a id="4.4"></a>

        ```bash
        queue.buffering.max.messages=2000000
        queue.buffering.max.kbytes=20971520
        queue.buffering.max.ms=5
        linger.ms=5
        batch.num.messages=200000
        batch.size=3145728
        max.push.records=10000
        ```

    5. Access `flink_tm2_8c32g` and perform steps [4.ii](#4.2) to [4.iv](#4.4).

        ```bash
        docker exec -it flink_tm2_8c32g /bin/bash
        ```

5. Start ZooKeeper and Kafka on the physical machine. For details, see [Kafka Deployment Guide](https://www.hikunpeng.com/document/detail/en/kunpengbds/ecosystemEnable/Kafka/kunpengkafka_04_0011.html).

6. Use Kafka to create topics and generate data.

    >![](public_sys-resources/icon-note.gif) **NOTE:**
    >Replace all the example IP addresses of physical machines in the commands or scripts with the actual IP addresses of the Kafka servers.

    1. Create topics for the source and sink.

        ```bash
        cd /usr/local/kafka
        bin/kafka-topics.sh --create --bootstrap-server IP_address_of_Kafka_server's_physical_machine:9092 --replication-factor 1 --partitions 1 --topic source_abcd
        bin/kafka-topics.sh --create --bootstrap-server IP_address_of_Kafka_server's_physical_machine:9092 --replication-factor 1 --partitions 1 --topic result
        ```

    2. Save the following content as the script file **producer.sh**.

        ```bash
        #!/bin/bash
        
        # Kafka installation directory (Replace the example directory with the actual one.)
        KAFKA_HOME="/usr/local/kafka"
        TOPIC_NAME="source_abcd"  # Kafka topic name
        BROKER="IP_address:9092"  # IP address of the Kafka broker server
        MESSAGE_COUNT=10             # Number of sent messages
        
        # Check whether **Kafka console-producer.sh** exists.
        if [ ! -f "$KAFKA_HOME/bin/kafka-console-producer.sh" ]; then
            echo "Error: kafka-console-producer.sh was not found. Check the KAFKA_HOME path."
            exit 1
        fi
        
        # Generate a random character string and send it to Kafka.
        for ((i=1; i<=$MESSAGE_COUNT; i++)); do
            # Generate four random letters (case-sensitive) + Space + 1.
            RAND_STR=$(cat /dev/urandom | tr -dc 'a-d' | fold -w 4 | head -n 1)
            MESSAGE="${RAND_STR} 1"  # Format: 4 letters + Space + 1
        
        # Invoke the Kafka producer to send messages.
            echo "$MESSAGE" | "$KAFKA_HOME/bin/kafka-console-producer.sh" \
                --bootstrap-server "$BROKER" \
                --topic "$TOPIC_NAME"
            echo "Sent: $MESSAGE"
        done
        ```

    3. Run the script to generate test data and write it to the source topic.

        ```bash
        ./producer.sh
        ```

7. Build a job JAR package.
    1. Go to the `/opt` directory on the physical machine and create the `/opt/job/src/main/java/com/huawei/boostkit` directory.

        ```bash
        mkdir -p /opt/job/src/main/java/com/huawei/boostkit
        cd /opt/job/
        ```

    2. Create a Java file for the Flink Job.
        1. Open `/opt/job/src/main/java/com/huawei/boostkit/FlinkWordCount.java`.

            ```bash
            vi /opt/job/src/main/java/com/huawei/boostkit/FlinkWordCount.java
            ```

        2. Press `i` to enter the insert mode and add the following content:

            ```bash
            package com.huawei.boostkit;
            
            import org.apache.flink.api.common.eventtime.WatermarkStrategy;
            import org.apache.flink.api.common.serialization.SimpleStringSchema;
            import org.apache.flink.connector.base.DeliveryGuarantee;
            import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
            import org.apache.flink.connector.kafka.sink.KafkaSink;
            import org.apache.flink.connector.kafka.source.KafkaSource;
            import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
            import org.apache.flink.streaming.api.datastream.DataStream;
            import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
            import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
            import org.apache.kafka.clients.consumer.ConsumerConfig;
            import org.apache.kafka.clients.producer.ProducerConfig;
            import org.apache.kafka.common.serialization.ByteArrayDeserializer;
            
            import java.util.Properties;
            
            public class FlinkWordCount {
                public static void main(String[] args) throws Exception {
                    String broker = "ip:port";
                    String sourceTopic = "source_abcd";
                    String targetTopic = "result";
                    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                    env.setParallelism(1);
                    Properties properties = new Properties();
                    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
                    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
                    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                        ByteArrayDeserializer.class.getCanonicalName());
                    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                        ByteArrayDeserializer.class.getCanonicalName());
                    KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                        .setBootstrapServers(broker)
                        .setTopics(sourceTopic)
                        .setGroupId("your-group-id")
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .setProperties(properties)
                        .build();
                    
                    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
                    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                        "org.apache.kafka.common.serialization.ByteArraySerializer");
                    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                        "org.apache.kafka.common.serialization.ByteArraySerializer");
                    properties.put(ProducerConfig.ACKS_CONFIG, "0");
                    properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
                    properties.put(ProducerConfig.CLIENT_ID_CONFIG, "DataGenerator");
                    KafkaSink<String> sink = KafkaSink.<String>builder()
                        .setBootstrapServers(broker)
                        .setRecordSerializer(
                            KafkaRecordSerializationSchema.builder()
                                .setTopic(targetTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .setKafkaProducerConfig(properties)
                        .build();
                    DataStream<String> source;
                    source = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source").disableChaining();
                    SingleOutputStreamOperator<String> result = source.map(line ->
                        line
                    );
                    result.sinkTo(sink);
                    result.disableChaining();
                    env.execute("Wordcount");
                }
            }
            ```

        3. Press `Esc`, type `:wq!`, and press `Enter` to save the file and exit.

    3. Create the `pom.xml` file.
        1. Open `/opt/job/pom.xml`.

            ```bash
            vi /opt/job/pom.xml
            ```

        2. Press `i` to enter the insert mode and add the following content:

            ```bash
            <?xml version="1.0" encoding="UTF-8"?>
            <project xmlns="http://maven.apache.org/POM/4.0.0"
                     xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                     xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
                <modelVersion>4.0.0</modelVersion>
            
                <groupId>com.huawei.boostkit</groupId>
                <artifactId>ziliao</artifactId>
                <version>1.0-SNAPSHOT</version>
                <packaging>jar</packaging>
            
                <properties>
                   <flink.version>1.16.3</flink.version>
                   <maven.compiler.source>1.8</maven.compiler.source>
                   <maven.compiler.target>1.8</maven.compiler.target>
                </properties>
            
                <dependencies>
                   <!-- Flink dependencies -->
                   <dependency>
                      <groupId>org.apache.flink</groupId>
                      <artifactId>flink-java</artifactId>
                      <version>${flink.version}</version>
                      <exclusions>
                         <exclusion>
                            <groupId>org.lz4</groupId>
                            <artifactId>lz4</artifactId>
                         </exclusion>
                      </exclusions>
                   </dependency>
                   <dependency>
                      <groupId>org.apache.flink</groupId>
                      <artifactId>flink-streaming-java</artifactId>
                      <version>${flink.version}</version>
                      <exclusions>
                         <exclusion>
                            <groupId>org.lz4</groupId>
                            <artifactId>lz4</artifactId>
                         </exclusion>
                      </exclusions>
                   </dependency>
                   <dependency>
                      <groupId>org.apache.flink</groupId>
                      <artifactId>flink-clients</artifactId>
                      <version>${flink.version}</version>
                      <exclusions>
                         <exclusion>
                            <groupId>org.lz4</groupId>
                            <artifactId>lz4</artifactId>
                         </exclusion>
                      </exclusions>
                   </dependency>
                   <dependency>
                      <groupId>org.apache.flink</groupId>
                      <artifactId>flink-connector-kafka</artifactId>
                      <version>${flink.version}</version>
                      <exclusions>
                         <exclusion>
                            <groupId>org.lz4</groupId>
                            <artifactId>lz4</artifactId>
                         </exclusion>
                      </exclusions>
                   </dependency>
                   <dependency>
                      <groupId>org.apache.kafka</groupId>
                      <artifactId>kafka-clients</artifactId>
                      <version>2.5.0</version>
                      <exclusions>
                         <exclusion>
                            <groupId>org.lz4</groupId>
                            <artifactId>lz4</artifactId>
                         </exclusion>
                      </exclusions>
                   </dependency>
                   <dependency>
                      <groupId>org.apache.flink</groupId>
                      <artifactId>flink-statebackend-rocksdb</artifactId>
                      <version>1.16.3</version>
                   </dependency>
                </dependencies>
            
                <build>
                   <plugins>
                      <plugin>
                         <groupId>org.apache.maven.plugins</groupId>
                         <artifactId>maven-assembly-plugin</artifactId>
                         <version>3.3.0</version>
                         <configuration>
                            <descriptorRefs>
                               <descriptorRef>jar-with-dependencies</descriptorRef>
                            </descriptorRefs>
                         </configuration>
                         <executions>
                            <execution>
                               <id>make-assembly</id>
                               <phase>package</phase>
                               <goals>
                                  <goal>single</goal>
                               </goals>
                            </execution>
                         </executions>
                      </plugin>
                   </plugins>
                </build>
            
            </project>
            ```

        3. Press `Esc`, type `:wq!`, and press `Enter` to save the file and exit.

    4. After the `mvn clean package` command is executed, the `ziliao-1.0-SNAPSHOT-jar-with-dependencies.jar` file is generated in the `target` directory. Upload the JAR package to the `/usr/local/flink` directory in the `flink\_jm\_8c32g` container.

        ```bash
        mvn clean package
        docker cp /opt/job/target/ziliao-1.0-SNAPSHOT-jar-with-dependencies.jar flink_jm_8c32g:/usr/local/flink
        ```

8. Export environment variables from the `flink\_jm\_8c32g` container.

    ```bash
    export CPLUS_INCLUDE_PATH=${JAVA_HOME}/include/:${JAVA_HOME}/include/linux:/opt/udf-trans-opt/libbasictypes/include:/opt/udf-trans-opt/libbasictypes/OmniStream/include:/opt/udf-trans-opt/libbasictypes/include/libboundscheck:/opt/udf-trans-opt/libbasictypes/OmniStream/core/include:/usr/local/ksl/include:$CPLUS_INCLUDE_PATH
    export C_INCLUDE_PATH=${JAVA_HOME}/include/:${JAVA_HOME}/include/linux:/opt/udf-trans-opt/libbasictypes/include:/opt/udf-trans-opt/libbasictypes/OmniStream/include:/opt/udf-trans-opt/libbasictypes/include/libboundscheck:/opt/udf-trans-opt/libbasictypes/OmniStream/core/include:/usr/local/ksl/include:$C_INCLUDE_PATH
    export LIBRARY_PATH=${JAVA_HOME}/jre/lib/aarch64:${JAVA_HOME}/jre/lib/aarch64/server:/opt/udf-trans-opt/libbasictypes/lib:/usr/local/ksl/lib:$LIBRARY_PATH
    export LD_LIBRARY_PATH=${JAVA_HOME}/jre/lib/aarch64:${JAVA_HOME}/jre/lib/aarch64/server:/opt/udf-trans-opt/libbasictypes/lib:/usr/local/ksl/lib:$LD_LIBRARY_PATH
    ```

9. Modify the UDF configuration file.
    1. Set the test case package name (`udf\_package`) and main class name (`main\_class`).

        ```bash
        vim /opt/udf-trans-opt/udf-translator/conf/udf_tune.properties
        ```

    2. Press `i` to enter the insert mode and modify `udf\_package` and `main\_class` as follows:

        ```bash
        udf_package=com.huawei.boostkit
        main_class=com.huawei.boostkit.FlinkWordCount
        ```

    3. Press `Esc`, type `:wq!`, and press `Enter` to save the file and exit.

10. Translate the test case JAR package.

    ```bash
    sh /opt/udf-trans-opt/udf-translator/bin/udf_translate.sh /usr/local/flink/ziliao-1.0-SNAPSHOT-jar-with-dependencies.jar flink
    ```

11. Submit a job in the `flink\_jm\_8c32g` container.

    ```bash
    cd /usr/local/flink
    bin/flink run -c com.huawei.boostkit.FlinkWordCount ziliao-1.0-SNAPSHOT-jar-with-dependencies.jar
    ```

12. View the sink topic data.
  
    Consume Kafka data and check whether the job is running properly.

    ```bash
    cd /usr/local/kafka
    bin/kafka-console-consumer.sh --bootstrap-server IP_address_of_server's_physical_machine:9092 --topic result --from-beginning
    ```

    ![](figures/en-us_image_0000002549520819.png)

13. In the `flink\_jm\_8c32g` container, view the latest Flink client log `flink-root-client-xxx.log`.

    ```bash
    cd /usr/local/flink-1.16.3/log
    ```

    If no error information is displayed, OmniStream is enabled successfully.

    ![](figures/en-us_image_0000002518120980.png)

## Maintaining the Feature<a name="ZH-CN_TOPIC_0000002517961044"></a>

Follow the operating instructions when upgrading or uninstalling OmniStream.

**Upgrading the Software<a name="section1255684918527"></a>**

>![](public_sys-resources/icon-notice.gif) **NOTICE**
>The upgrade cannot be performed using the tool. Therefore, you need to download the installation package and reinstall the software.

Download the OmniStream software installation package from the [Kunpeng community](https://www.hikunpeng.com/zh/developer/download?title=%E5%A4%A7%E6%95%B0%E6%8D%AE&subTitle=OmniRuntime).

**Uninstalling the Software<a name="section1939611410533"></a>**

>![](public_sys-resources/icon-notice.gif) **NOTICE**
>
>- This step is optional and is not mandatory for deploying OmniStream.
>- Before uninstalling OmniStream, ensure that the Flink engine is not executing any tasks.

The following steps assume that the installation directories are `/opt/Dependency_library` and `/usr/local/OmniStream`.

1. Delete software dependency packages from `/opt/Dependency_library` and `/usr/local/OmniStream`.
2. Modify the `config.sh` file in the `$FLINK_HOME/bin` directory to restore the default Flink configuration.

    Restore step 3 in [Installation Guide–Installing OmniStream](installation_guide.md) to its original state.

3. Modify the `flink-conf.yaml` file in the `$FLINK_HOME/conf` directory to restore the default Flink configuration.

    Restore step 4 in [Installation Guide–Installing OmniStream](installation_guide.md) to its original state.
