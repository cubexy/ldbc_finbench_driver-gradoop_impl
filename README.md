![LDBC Logo](ldbc-logo.png)

![GRADOOP Logo](gradoop-logo.png)

# *LDBC FinBench Driver* - GRADOOP Implementation

The LDBC FinBench Driver is a powerful tool designed for benchmarking the performance of graph databases. This is the alpha version of the FinBench driver, currently undergoing alpha testing.

## 1. Configurations

The Driver initiates by reading the configuration file. The file comes pre-filled with configuration data `src/main/resources/gradoop/ldbc_finbench_driver_gradoop.properties`.

### 1.1 Mode

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

Start the Docker Compose environment located inside `/flink` with the pre-bundled SF0.01 dataset:

```bash
cd flink
docker-compose build --no-cache
docker-compose up -d
```

Fill the configuration properties as specified in the comments above the variables. Set the `queries.parameters_dir` and the `queries.updates_dir` to the read_params and incremental folder of the SF0.01 dataset on your machine (not HDFS!).

Then execute the Driver class.

## 3. Reference

- FinBench Specification: https://github.com/ldbc/ldbc_finbench_docs
- FinBench DataGen: https://github.com/ldbc/ldbc_finbench_datagen
- FinBench Driver: https://github.com/ldbc/ldbc_finbench_driver
- FinBench Transaction Reference Implementation: https://github.com/ldbc/ldbc_finbench_transaction_impls 

Please visit these links for further documentation and related resources.
