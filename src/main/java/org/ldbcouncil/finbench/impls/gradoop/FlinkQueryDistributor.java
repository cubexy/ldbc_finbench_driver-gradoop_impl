package org.ldbcouncil.finbench.impls.gradoop;

import java.io.File;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.client.program.ProgramMissingJobException;
import org.apache.logging.log4j.Logger;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;

public class FlinkQueryDistributor {

    private final RestClusterClient<StandaloneClusterId> client;

    public FlinkQueryDistributor(String flinkEndpoint, int flinkEndpointPort,
                                 String flinkQueryDistributorJarClassName,
                                 String flinkQueryExecutorJar, Logger logger) throws Exception {
        this.client = initClient(flinkEndpoint, flinkEndpointPort);
    }

    /**
     * Initializes the client.
     *
     * @param hostname hostname of the Flink cluster
     * @param port     port of the Flink cluster
     * @return RestClusterClient
     * @throws Exception error while initializing the client
     */
    private RestClusterClient<StandaloneClusterId> initClient(String hostname, int port) throws Exception {
        Configuration config = new Configuration();
        config.setString(JobManagerOptions.ADDRESS, hostname);
        config.setInteger(RestOptions.PORT, port);

        return new RestClusterClient<StandaloneClusterId>(config, StandaloneClusterId.getInstance());
    }

    /**
     * Runs a Flink job.
     *
     * @param parallelism number of parallel tasks
     * @param jarFilePath path to the jar file
     * @param args        arguments for the jar file
     * @throws ProgramInvocationException error while running the job
     * @throws ProgramMissingJobException  error while running the job
     */
    public void runJobOnClient(int parallelism, String jarFilePath, String[] args)
        throws ProgramInvocationException, ProgramMissingJobException {
        PackagedProgram packagedProgram = new PackagedProgram(new File(jarFilePath), args);
        JobSubmissionResult result = this.client.run(packagedProgram,  parallelism);
    }

}
