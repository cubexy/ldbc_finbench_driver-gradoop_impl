package org.ldbcouncil.finbench.impls.gradoop;

import com.nextbreakpoint.flink.client.api.FlinkApi;
import java.time.Duration;

public class FlinkQueryDistributor {
    private FlinkApi api; // https://github.com/nextbreakpoint/flink-client

    public FlinkQueryDistributor(String flinkEndpoint, String flinkQueryDistributorJarClassName,
                                 String flinkQueryExecutorJarPath) {
        this.api = initFlinkApi();
    }

    private FlinkApi initFlinkApi() {
        FlinkApi api = new FlinkApi();
        api.getApiClient()
            .setHttpClient(
                api.getApiClient()
                    .getHttpClient()
                    .newBuilder()
                    .connectTimeout(Duration.ofSeconds(20))
                    .writeTimeout(Duration.ofSeconds(30))
                    .readTimeout(Duration.ofSeconds(30))
                    .build()
            );
        return api;
    }
}
