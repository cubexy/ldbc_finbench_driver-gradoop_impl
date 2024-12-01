![LDBC Logo](ldbc-logo.png)

![GRADOOP Logo](gradoop-logo.png)

# *LDBC FinBench Driver* - GRADOOP Implementation

![Build status](https://github.com/ldbc/ldbc_finbench_driver/actions/workflows/ci.yml/badge.svg?branch=main)

The LDBC FinBench Driver is a powerful tool designed for benchmarking the performance of graph databases. This is the alpha version of the FinBench driver, currently undergoing alpha testing.

## 1. Configurations

The Driver initiates by reading the configuration file. The file comes pre-filled with configuration data `src/main/resources/gradoop/ldbc_finbench_driver_gradoop.properties`.
The Driver can run in two modes: `standalone` and `cluster`.

#### Standalone mode (gradoop_cluster_execution=false)

In Standalone mode, the driver only executes in a Flink MiniCluster locally. This can be useful for debugging.

#### Cluster mode (gradoop_cluster_execution=true)

In Cluster mode, the driver only starts the individual queries locally and then sends the job to a Gradoop cluster.
Note: Please specify the cluster URL before using Cluster mode!

### 1.1 Flink

Before running the `gradoop_cluster_execution` mode, please upload a prebuilt JAR to the SUT Flink cluster.

### 1.2 Mode

The driver operates in three modes:

- CREATE_VALIDATION
- VALIDATE_DATABASE
- EXECUTE_BENCHMARK

#### CREATE_VALIDATION

With `CREATE_VALIDATION` mode, you create a database result. `validation_parameters_size` denotes the number of results created, while `validate_database` refers to the file where the created results are stored.

```shell
mode=CREATE_VALIDATION
validation_parameters_size=100
validate_database=validation_params.csv
```

#### VALIDATE_DATABASE

`VALIDATE_DATABASE` mode allows you to verify the SUT. The `validate_database` is the result created by `CREATE_VALIDATION` mode.

```shell
mode=VALIDATE_DATABASE
validate_database=validation_params.csv
```

#### EXECUTE_BENCHMARK

Perform the performance test with `EXECUTE_BENCHMARK` mode.  Here are some crucial configuration parameters that need adjustment when operating the driver:

1. **thread_count**: Represents the number of concurrent requests that the driver can handle, corresponding to the number of active threads running simultaneously within the driver.
2. **time_compression_ratio**: Controls the intensity of the driver's workload. A lower value yields a higher workload in a shorter timeframe.
3. **ignore_scheduled_start_times**: Determines whether the driver should follow the scheduled timings for sending requests. If set to true, the driver sends requests as soon as they are prepared, regardless of the schedule.
4. **warmup**: Denotes the number of preliminary test items processed before the actual benchmarking begins.
5. **operation_count**: Sets the number of test items executed during the actual benchmarking phase after the warm-up. 

```shell
mode=EXECUTE_BENCHMARK
thread_count=1
time_compression_ratio=0.001
ignore_scheduled_start_times=false
warmup=5
operation_count=10000
```

## 2. Quick Start

To get started, clone the repository and build it with Maven:

```bash
mvn clean package -DskipTests
```

Fill the configuration properties as specified in the comments above the variables.

#### For Cluster mode:

Start the docker-compose file inside the `flink` folder and upload the test files to HDFS. Note: You may need to tap into the datanode container and adjust read permissions manually to be able to create folders and upload files. Also upload the JAR to Flink.

Then execute the Driver class.

## 3. Reference

- FinBench Specification: https://github.com/ldbc/ldbc_finbench_docs
- FinBench DataGen: https://github.com/ldbc/ldbc_finbench_datagen
- FinBench Driver: https://github.com/ldbc/ldbc_finbench_driver
- FinBench Transaction Reference Implementation: https://github.com/ldbc/ldbc_finbench_transaction_impls 

Please visit these links for further documentation and related resources.
