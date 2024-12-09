package org.ldbcouncil.finbench.impls.gradoop;

import com.nextbreakpoint.flink.client.api.ApiException;
import com.nextbreakpoint.flink.client.api.FlinkApi;
import com.nextbreakpoint.flink.client.model.JarListInfo;
import java.time.Duration;
import org.apache.logging.log4j.Logger;

public class FlinkQueryDistributor {
    private FlinkApi api; // https://github.com/nextbreakpoint/flink-client

    public FlinkQueryDistributor(String flinkEndpoint, String flinkQueryDistributorJarClassName,
                                 String flinkQueryExecutorJar, Logger logger) {
        this.api = initFlinkApi(flinkEndpoint, logger);
    }

    private FlinkApi initFlinkApi(String flinkEndpoint, Logger logger) {
        logger.info("Initializing Flink query distributor...");
        FlinkApi api = new FlinkApi();
        api.getApiClient()
            .setBasePath(flinkEndpoint)
            .setHttpClient(
                api.getApiClient()
                    .getHttpClient()
                    .newBuilder()
                    .connectTimeout(Duration.ofSeconds(20))
                    .writeTimeout(Duration.ofSeconds(30))
                    .readTimeout(Duration.ofSeconds(30))
                    .build()
            );
        try {
            JarListInfo jars = api.getJarList();
            logger.info("available jars: {}", jars.getFiles());
        } catch (ApiException e) {
            throw new RuntimeException(e);
        }
        logger.info("Flink query distributor initialized.");
        return api;
    }
}
