# Introduction to OmniStream<a name="ZH-CN_TOPIC_0000002549521549"></a>

## Latest Updates<a name="ZH-CN_TOPIC_0000002517961774"></a>

- \[2026-06-30\]: Released OmniStream 1.3.0. Added the following support in SQL scenarios: WindowAgg and WindowJoin operators are supported. The Calc operator supports UDF registration, as well as built-in functions such as JSON_VALUE, JSON_QUERY, COALESCE, PROCTIME_MATERIALIZE, CHAR_LENGTH, and TO_TIMESTAMP_LTZ. The Calc operator also supports data types including INTEGER and TIMESTAMP_WITH_LOCAL_TIMEZONE(3).
- \[2026-03-30\]: Released OmniStream 1.2.0. Added the content of installing the header file on which the UDF translator depends, and introduced support for enabling the OmniStateStore feature in stateful scenarios.
- \[2025-12-30\]: Released OmniStream 1.1.0. Added the task-level operator rollback mechanism in SQL scenarios, and added checkpoint and restore support for KeyedCoProcess in DataStream scenarios.
- \[2025-06-30\]: Released OmniStream 1.0.0. SQL: Implemented acceleration for the Calc, GroupAgg, Join, Deduplicate, Rank, Window, and Kafka Source/Sink operators; implemented the efficient data organization method OmniVec; added support for memory and RocksDB state backends. DataStream: Implemented acceleration for the Kafka Source, Kafka Sink, Map, FlatMap, Reduce, and filter operators; implemented the UDF basic framework and UDF translation basic library; added support for the UDF automatic native framework to run stateful and stateless cases such as DataStream Wordcount; added support for the memory state backend.

## Project Description<a name="ZH-CN_TOPIC_0000002517961778"></a>

### Overview<a name="ZH-CN_TOPIC_0000002549641551"></a>

The big data features of OmniRuntime are presented in the form of plugins to improve the performance of data loading, computing, and exchange from end to end.

Data volumes generated from Internet services have been growing much faster than CPUs' computing power. The open-source big data ecosystem is also developing on a fast track. However, diversified computing engines and open-source components make it difficult to improve data processing performance throughout the lifecycle. Different big data engines use their own unique tuning policies and techniques to improve performance and efficiency. Some tuning items may be applied across multiple engines, which may cause resource contention and conflicts, reducing overall computing performance.

OmniRuntime consists of a series of features provided by Kunpeng BoostKit for Big Data in terms of application acceleration. It aims to improve the performance of end-to-end data loading, computing, and exchange through plugins, thereby improving the performance of big data analytics.

As a subfeature of OmniRuntime, OmniStream uses native code (C/C++) to implement Flink SQL operators to improve query performance. The Flink engine is reconstructed natively for enhanced performance.

It has been adapted to Flink 1.16.3.

### Architecture<a name="ZH-CN_TOPIC_0000002549521547"></a>

The OmniStream feature uses native code (C/C++) to reconstruct the logic of Flink SQL and DataStream operators, improving query performance.

- For SQL, OmniStream uses C++ and vectorized instructions to implement operators, leveraging vectorization to enhance SQL computing performance.
- For DataStream, OmniStream uses C++ and vectorized instructions to implement operators, fully leveraging the performance advantages of native code to improve performance in DataStream scenarios.

In stateful scenarios, OmniStream enables the OmniStateStore feature based on a native architecture, reducing RocksDB access frequency and drive I/O to improve query performance.

- For RocksDBValueState, state aggregation is performed in memory through state caching, reducing RocksDB access frequency.
- For RocksDBValueState, the memTable data structure is redesigned as a LinkHashList to improve state point read/write efficiency.
- For RocksDBMapState, prefix filters are applied to eliminate redundant drive lookups, improving range query performance.
- For all state types, filter sizes at different levels are dynamically adjusted to reduce redundant drive lookups and improve state point read performance.

**SQL<a name="zh-cn_topic_0000002543922607_section6310183119441"></a>**

OmniStream uses an architecture consisting of the Java Adapter layer and the CPP Core layer.

- The Java Adapter layer is implemented in Java and is responsible for generating native execution plans and falling back to the Java runtime in unsupported scenarios.
- The CPP Core layer is implemented in C++ and is responsible for implementing operator logic and data transmission.

An SQL query submitted through SQL or Table API is parsed, and an execution plan is then generated. The Java Adapter layer obtains the execution plan, initializes related tasks in the CPP, and generates an operator chain. After the initialization process is complete, the task runs to read data from the source. After a series of operator processing operations, Sink outputs the result.

[Figure 1](#architecture-of-omnistream-flink-sql-native) shows the OmniStream Flink SQL Native architecture.<br><br>
**Figure 1** OmniStream Flink SQL Native architecture<a name="zh-cn_topic_0000002543922607_zh-cn_topic_0000002228744542_fig685102835814"></a><a id="architecture-of-omnistream-flink-sql-native"></a><br>
![](docs/en/figures/architecture-of-omnistream-flink-sql-native.png "OmniStream Flink SQL Native architecture")

**DataStream<a name="zh-cn_topic_0000002543922607_section10781121012457"></a>**

After receiving an input, the DataStream API parses it into an execution plan. The Java adapter layer parses the plan, initializes related tasks on the C++ side, and builds the corresponding operator chain. After the initialization process is complete, the task runs to read data from the source. After a series of operator processing operations, Sink outputs the result.

[Figure 2](#architecture-of-omnistream-flink-datastream-native) shows the OmniStream Flink DataStream Native architecture.<br>

**Figure 2** OmniStream Flink DataStream Native architecture<a name="zh-cn_topic_0000002543922607_fig918683363812"></a><a id="architecture-of-omnistream-flink-datastream-native"></a><br>
![](docs/en/figures/architecture-of-omnistream-flink-datastream-native.png "OmniStream Flink DataStream Native architecture")

**OmniStateStore<a name="zh-cn_topic_0000002543922607_section10781121012457"></a>**

When the RocksDB state backend is used in OmniStream SQL or DataStream scenarios, the OmniStateStore feature is automatically enabled. It leverages techniques such as state aggregation and state filtering to reduce RocksDB access frequency, lower drive I/O overhead, and improve end-to-end application throughput.

[Figure 3](#architecture-of-omnistream-omniStateStore) shows the OmniStream Flink OmniStateStore Native architecture.<br>

**图 3** OmniStream Flink OmniStateStore Native architecture<a name="zh-cn_topic_0000002543922607_zh-cn_topic_0000002228744542_fig685102835814"></a><a id="architecture-of-omnistream-omniStateStore"></a><br>
![](docs/en/figures/architecture-of-omnistream-omniStateStore.png "OmniStream Flink OmniStateStore Native architecture")

## Constraints

OmniStream has restrictions on supported data types, operators, and state backends. Plan your tasks accordingly and avoid unsupported scenarios.

**SQL<a name="zh-cn_topic_0000002512242608_section9207153194615"></a>**

- OmniStream Flink Native supports the Nexmark benchmarking suite, including Nexmark data types and built-in functions.
- Supported data types: BIGINT, TIMESTAMP(3), and VARCHAR.
- Supported expressions: + - * /
- Supported built-in functions: LOWER, SPLIT\_INDEX, DATE\_FORMAT, MOD, and COUNT\_CHAR.
- Supported GroupAggregate functions: SUM\(BIGINT\), COUNT\(BIGINT\), AVG \(BIGINT\), MIN\(BIGINT\), MIN\(VARCHAR\), MAX \(BIGINT\), and MAX\(VARCHAR\).
- The join key of the join operator must be of the BIGINT type, and the operation type must be Inner Join.
- The Deduplicate and Rank operators allow only Partition By BIGINT. All fields in the query table must be of the supported data type and only the ROW\_NUMBER function is supported. `Partition By` of Rank supports only one field of the BIGINT type. `Order By` of TOPN supports only one BIGINT field, whose sorting rule is DESC. `Order By` of TOP1 supports a maximum of two fields. The type can be BIGINT TIMESTAMP\(3\), and the sorting rule can be DESC or ASC.
- The Group By column of the Aggregate operator allows only BIGINT.
- The aggregate function of the LocalWindowAGG and GlobalWindowAGG operators is COUNT, SUM, MIN, or MAX. The aggregate function of the GroupWindowAGG operator is COUNT, SUM, MIN, or MAX.
- The LocalWindowAGG and GlobalWindowAGG operators support only the TUMBLE and HOP windows.
- The GroupWindowAGG operator supports only the SESSION, TUMBLE, and HOP window.
- The external table data source of the Lookup Join operator supports only CSV files.
- The state backend supports only the memory and RocksDB.
- Flink stores states in the memory state backend, and the memory usage grows over time as the volume of data increases. In comparison, OmniStream uses the columnar vectorized architecture to optimize performance. Its state storage behaves the same as the native Flink while delivering a higher processing speed and consuming the memory space faster. Therefore, the Nexmark benchmark test cases support a maximum of 50 million data records.
- In SQL scenarios, checkpoints and savepoints are supported. The related configuration options and commands are consistent with those of the open-source Flink community.
- In SQL scenarios, when Kafka is used as a data source, running in multi-parallelism mode is not supported. In other scenarios, In other scenarios, high parallelism is currently supported only for stateless operators.
- In SQL scenarios, OmniStream supports saving savepoints in Flink standard format via the command line `./flink savepoint/stop --type [compatible]`. Currently, only Join, Deduplicate, and Rank operators are supported.
- In SQL scenarios, OmniStream supports restoring savepoints in Flink standard format via the parameter `"SET 'omni.recovery.savepoint.format' = 'compatible'"`. Currently, only Join, Deduplicate, and Rank operators are supported.

**DataStream<a name="zh-cn_topic_0000002512242608_section14862183194614"></a>**

For details, see [Supported DataStream operators and UDFs](docs/en/user_guide.md#supported-datastream-operators-and-udfs).

- Source and Sink support only Kafka data sources.
- These operators are supported: Map, FlatMap, GroupReduce, Filter, Source, and Sink.
- The Filter operator must be RichFilterFunction.
- Checkpoints and savepoints can only be created when using the in-memory or RocksDB state backends. The related configuration options and commands are consistent with those of the open-source Flink community.

**OmniStateStore<a name="zh-cn_topic_0000002512242608_section14862183194614"></a>**

- Supported acceleration features include Flink semantic state caching, dynamic filtering, and intelligent multi-stream awareness.
- These acceleration features are enabled by default in OmniStream and cannot be disabled.

### Application Scenarios<a name="ZH-CN_TOPIC_0000002549521541"></a>

Apache Flink is an open-source real-time stream processing engine. As services rapidly evolve and data volumes surge, Flink's performance bottlenecks have started to surface in certain high-load scenarios. This is particularly evident in Internet-related use cases, where Flink trails some peer offerings in terms of performance. OmniStream uses native code (C/C++) to implement Flink SQL operators, improving query execution efficiency. It natively reconstructs the Flink engine to enhance performance.

OmniStream supports Flink 1.16.3. It parses user-submitted SQL statements into a series of operators and executes them using native operators provided by it. These native operators replace open source Flink operators, significantly improving performance.

OmniStream improves the processing performance of the Flink engine while preserving existing development practices and architectural compatibility. In large-scale real-time data analysis scenarios, it significantly enhances processing capacity and execution efficiency.

### Concepts<a name="ZH-CN_TOPIC_0000002518121694"></a>

Nexmark: It is a benchmark suite designed for evaluating queries on continuous data streams. It offers fair and comprehensive performance tests for stream processing systems, serving as a tool for both optimization guidance and performance comparison.

## Directory Structure<a name="ZH-CN_TOPIC_0000002518121690"></a>

The full project directory structure is as follows:

```text
├─cpp
│  ├─conf
│  ├─connector
│  ├─core
│  ├─datagen
│  ├─include
│  ├─jni
│  ├─runtime
│  ├─streaming
│  ├─tabler
│  ├─test
│  ├─third_party
│  ├─translate
│  └─zemo
├─docs                                                       # Project document directory
│   ├── en                                                   # Document directory
│   │   ├── figures                                          # Images in documents
│   │   ├── public_sys-resources                             # Icons in the document
│   │   ├── faq.md                                           # OmniStream Installation FAQs
│   │   ├── installation_guide.md                            # OmniStream Installation Guide
│   │   ├── quick_start.md                                   # OmniStream Quick Start
│   │   ├── release_notes.md                                 # OmniStream Release Notes
│   │   └── user_guide.md                                    # OmniStream User Guide
├─README_en.md
└─scripts
```

## Version Description<a name="ZH-CN_TOPIC_0000002549521545"></a>

For details about feature changes in each version, see [Release Notes](./docs/en/release_notes.md).

## Environment Setup<a name="ZH-CN_TOPIC_0000002518121692"></a>

For details about the environment dependencies and installation methods of OmniStream, see [Installation Guide](./docs/en/installation_guide.md).

## Quick Start<a name="ZH-CN_TOPIC_0000002517961786"></a>

For instructions on quickly verifying whether OmniStream is active and its performance improvements, see [Quick Start](./docs/en/quick_start.md).

## Related Documents<a name="ZH-CN_TOPIC_0000002549641549"></a>

|Name|Description|
|--|--|
|[Quick Start](./docs/en/quick_start.md)|Provides guidance on how to quickly enable and verify the OmniStream feature.|
|[Release Notes](./docs/en/release_notes.md)|Provides basic information and feature updates of each OmniStream version.|
|[Installation Guide](./docs/en/installation_guide.md)|Describes how to install OmniStream.|
|[User Guide](./docs/en/user_guide.md)|Provides details about how to use OmniStream.|
|[FAQs](./docs/zh/faq.md)|Provides answers to frequently asked questions (FAQs) about installing and using OmniStream.|

## Security Statement<a name="ZH-CN_TOPIC_0000002551517383"></a>

### Routine Antivirus Software Check<a name="ZH-CN_TOPIC_0000002520637378"></a>

Periodically scan clusters and Spark components for viruses. This protects clusters from viruses, malicious code, spyware, and malicious programs, reducing risks such as system breakdown and information leakage. Mainstream antivirus software is recommended for antivirus check.

### Log Control<a name="ZH-CN_TOPIC_0000002551637367"></a>

- Check whether the system can limit the size of a single log file.
- Check whether there is a mechanism for clearing logs when the log space is used up.

### Vulnerability fixing<a name="ZH-CN_TOPIC_0000002520477388"></a>

To ensure the security of the production environment and reduce attacks, enable the firewall and periodically fix the following vulnerabilities:

- OS vulnerabilities
- JDK vulnerabilities
- Hadoop and Spark vulnerabilities
- ZooKeeper vulnerabilities
- Kerberos vulnerabilities
- OpenSSL vulnerabilities
- Vulnerabilities in other components

    The following uses CVE-2021-37137 as an example.

    Vulnerability description:

    Netty 4.1.17 has two Content-Length HTTP headers that may be confused. The vulnerability ID is CVE-2021-37137.

    The system uses the hdfs-ceph (version 3.2.0) service as the storage object with decoupled storage and compute. This service depends on `aws-java-sdk-bundle-1.11.375.jar` and involves this vulnerability. You are advised to update the vulnerability patch in a timely manner to prevent hacker attacks.

    Impact scope:

    Netty 4.1.68 and earlier versions

    Handling suggestion:

    The vendor has released an upgrade patch to fix the vulnerability. For details, visit [GitHub](https://github.com/netty/netty/security/advisories/GHSA-9vjp-v76f-g363).

### SSH Hardening<a name="ZH-CN_TOPIC_0000002551517385"></a>

During the installation and deployment, you need to connect to the server through SSH. The root user has all the operation permissions. Logging in to the server as the root user may pose security risks. You are advised to log in to the server as a common user for installation and deployment and disable root user login using SSH to improve system security.

Check the `PermitRootLogin` configuration item in `/etc/ssh/sshd\_config`.

- If the value is `no`, root user login using SSH is disabled.
- If the value is `yes`, change it to `no`.

### Public Network Address Statement<a name="ZH-CN_TOPIC_0000002520637380"></a>

**Table 1** Public network address statement<a id="public-network-address-statement"></a>

<a name="zh-cn_topic_0000002547269015_table5591719574"></a>
<table><tbody><tr id="zh-cn_topic_0000002547269015_row13592819778"><th class="firstcol" valign="top" width="30%" id="mcps1.2.3.1.1"><p id="zh-cn_topic_0000002547269015_p559212199711"><a name="zh-cn_topic_0000002547269015_p559212199711"></a><a name="zh-cn_topic_0000002547269015_p559212199711"></a>Open-Source/Third-Party Software</p>
</th>
<td class="cellrowborder" valign="top" width="70%" headers="mcps1.2.3.1.1 "><p id="zh-cn_topic_0000002547269015_p1259291919710"><a name="zh-cn_topic_0000002547269015_p1259291919710"></a><a name="zh-cn_topic_0000002547269015_p1259291919710"></a>GCC</p>
</td>
</tr>
<tr id="zh-cn_topic_0000002547269015_row959213199719"><th class="firstcol" valign="top" width="30%" id="mcps1.2.3.2.1"><p id="zh-cn_topic_0000002547269015_p25928193714"><a name="zh-cn_topic_0000002547269015_p25928193714"></a><a name="zh-cn_topic_0000002547269015_p25928193714"></a>Type</p>
</th>
<td class="cellrowborder" valign="top" width="70%" headers="mcps1.2.3.2.1 "><p id="zh-cn_topic_0000002547269015_p259214193711"><a name="zh-cn_topic_0000002547269015_p259214193711"></a><a name="zh-cn_topic_0000002547269015_p259214193711"></a>Open-source software</p>
</td>
</tr>
<tr id="zh-cn_topic_0000002547269015_row15921819775"><th class="firstcol" valign="top" width="30%" id="mcps1.2.3.3.1"><p id="zh-cn_topic_0000002547269015_p145921119774"><a name="zh-cn_topic_0000002547269015_p145921119774"></a><a name="zh-cn_topic_0000002547269015_p145921119774"></a>Public IP Address/Public URL/Domain Name/Email Address</p>
</th>
<td class="cellrowborder" valign="top" width="70%" headers="mcps1.2.3.3.1 "><p id="zh-cn_topic_0000002547269015_p8592141918714"><a name="zh-cn_topic_0000002547269015_p8592141918714"></a><a name="zh-cn_topic_0000002547269015_p8592141918714"></a><a href="https://gcc.gnu.org/bugs/" target="_blank" rel="noopener noreferrer">https://gcc.gnu.org/bugs/</a></p>
</td>
</tr>
<tr id="zh-cn_topic_0000002547269015_row559214191971"><th class="firstcol" valign="top" width="30%" id="mcps1.2.3.4.1"><p id="zh-cn_topic_0000002547269015_p1359219191070"><a name="zh-cn_topic_0000002547269015_p1359219191070"></a><a name="zh-cn_topic_0000002547269015_p1359219191070"></a>File Type</p>
</th>
<td class="cellrowborder" valign="top" width="70%" headers="mcps1.2.3.4.1 "><p id="zh-cn_topic_0000002547269015_p1059214191475"><a name="zh-cn_topic_0000002547269015_p1059214191475"></a><a name="zh-cn_topic_0000002547269015_p1059214191475"></a>Binary</p>
</td>
</tr>
<tr id="zh-cn_topic_0000002547269015_row185922197711"><th class="firstcol" valign="top" width="30%" id="mcps1.2.3.5.1"><p id="zh-cn_topic_0000002547269015_p7593619372"><a name="zh-cn_topic_0000002547269015_p7593619372"></a><a name="zh-cn_topic_0000002547269015_p7593619372"></a>File Name</p>
</th>
<td class="cellrowborder" valign="top" width="70%" headers="mcps1.2.3.5.1 "><p id="zh-cn_topic_0000002547269015_p1559317198713"><a name="zh-cn_topic_0000002547269015_p1559317198713"></a><a name="zh-cn_topic_0000002547269015_p1559317198713"></a>libboostkit-omniop-vector-2.0.0-aarch64.so</p>
</td>
</tr>
<tr id="zh-cn_topic_0000002547269015_row0593141917711"><th class="firstcol" valign="top" width="30%" id="mcps1.2.3.6.1"><p id="zh-cn_topic_0000002547269015_p1459319197711"><a name="zh-cn_topic_0000002547269015_p1459319197711"></a><a name="zh-cn_topic_0000002547269015_p1459319197711"></a>Usage</p>
</th>
<td class="cellrowborder" valign="top" width="70%" headers="mcps1.2.3.6.1 "><p id="zh-cn_topic_0000002547269015_p2059320191370"><a name="zh-cn_topic_0000002547269015_p2059320191370"></a><a name="zh-cn_topic_0000002547269015_p2059320191370"></a>This email address is the official address of the open-source component GCC, and is used only to compile the open-source component. This email address is not used inside this product.</p>
</td>
</tr>
<tr id="zh-cn_topic_0000002547269015_row259317197712"><th class="firstcol" valign="top" width="30%" id="mcps1.2.3.7.1"><p id="zh-cn_topic_0000002547269015_p195931919976"><a name="zh-cn_topic_0000002547269015_p195931919976"></a><a name="zh-cn_topic_0000002547269015_p195931919976"></a>Software Package</p>
</th>
<td class="cellrowborder" valign="top" width="70%" headers="mcps1.2.3.7.1 "><p id="zh-cn_topic_0000002547269015_p12877016104"><a name="zh-cn_topic_0000002547269015_p12877016104"></a><a name="zh-cn_topic_0000002547269015_p12877016104"></a>BoostKit-omniop_2.0.0.zip</p>
<p id="zh-cn_topic_0000002547269015_p1959311194714"><a name="zh-cn_topic_0000002547269015_p1959311194714"></a><a name="zh-cn_topic_0000002547269015_p1959311194714"></a>boostkit-omniop-operator-2.0.0-aarch64-centos.tar.gz</p>
</td>
</tr>
</tbody>
</table>

## Disclaimer<a name="ZH-CN_TOPIC_0000002549641559"></a>

**To OmniStream Users**

- This tool is intended solely for debugging and development. You are responsible for any risks and should carefully review the following information:
    - Data processing and deletion: Users are responsible for managing and deleting any data generated while using this tool. You are advised to promptly delete any related data after use to prevent information leaks.
    - Data confidentiality and transmission: Users understand and agree not to share or transmit any data generated by this tool. Neither the tool nor its developers are responsible for any information leaks, data breaches, or other negative consequences.
    - User input security: Users are responsible for the security of any commands they enter and for any risks or losses resulting from improper input. The tool and its developers are not liable for issues caused by incorrect command usage.

- Disclaimer scope: This disclaimer applies to all individuals and entities using this tool. By using the tool, you acknowledge and accept this statement and assume all risks and responsibilities arising from its use. If you do not agree, please stop using the tool immediately.
- Before using this tool, please **read and understand the preceding disclaimer**. If you have any questions, contact the developer.

**To Data Owners**

If you do not want your model or dataset to be mentioned in OmniStream, or if you wish to update its description, please submit an issue on GitCode. We will delete or update your description according to your request. Thank you for your understanding and contribution to OmniStream.

## License<a name="ZH-CN_TOPIC_0000002518121688"></a>

For details about the OmniStream license, see [LICENSE](./LICENSE).

Documents in the `docs` directory are licensed under CC-BY 4.0. For details, see [LICENSE](./docs/LICENSE).

## Contributor Statement<a name="ZH-CN_TOPIC_0000002549521553"></a>

1. Submit an error report: If you discover a vulnerability in OmniStream that is not a security issue, first search the **Issues** in the OmniStream repository to avoid submitting duplicates. If the vulnerability is not listed, create a new issue. If you discover a security-related issue, do not disclose it publicly. Please refer to the security handling guidelines for details. All error reports must include complete information about the issue.
2. Handling security issues: For guidance on handling security issues in this project, please contact the core team via email for instructions.
3. Resolving existing issues: Review the issue list of the repository to identify issues that need attention, and attempt to resolve them.
4. Proposing new features: Use the Feature label when creating an issue for a new feature. We will review and confirm proposals periodically.
5. How to contribute:
    1. Fork the repository of the project.
    2. Clone it to your local machine.
    3. Create a development branch.
    4. Conduct local testing. All unit tests, including any new test cases, must pass before submission.
    5. Submit your code.
    6. Create a pull request (PR).
    7. Code review: Modify the code according to review comments and resubmit your changes. This process may involve multiple rounds of iterations.
    8. After your PR is approved by the required number of reviewers, the committer will conduct the final review.
    9. After your PR is approved and all tests pass, the CI system will merge it into the main branch of the project.

## Suggestions and Communication<a name="ZH-CN_TOPIC_0000002549641561"></a>

You are welcome to contribute to the community. If you have any questions or suggestions, please submit an [issue](https://gitcode.com/boostkit/omnistream). We will reply as soon as possible. Thank you for your support.

## Acknowledgement<a name="ZH-CN_TOPIC_0000002549641563"></a>

Thank you to everyone in the community for your PRs. We warmly welcome contributions to OmniStream!
