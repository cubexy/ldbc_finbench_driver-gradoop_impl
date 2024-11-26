package org.ldbcouncil.finbench.impls.gradoop.queries.simple.read4;

import java.util.List;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.logging.log4j.Logger;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead4;
import org.ldbcouncil.finbench.impls.gradoop.FlinkCmdArg;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;

public class SimpleRead4CmdArgExecutor {
    public static void execute(FlinkCmdArg inputArgs, GradoopFinbenchBaseGraphState graph, Logger logger) throws DbException {
        logger.info("executing query SimpleRead4 with args {}", inputArgs);
        SimpleRead4 input = new SimpleRead4(inputArgs.getId(), inputArgs.getThreshold(), inputArgs.getStartTime(), inputArgs.getEndTime());
        SimpleRead4GradoopOperator operator = new SimpleRead4GradoopOperator(input);
        logger.info("started execution...");
        List<Tuple3<Long, Integer, Double>> result = operator.execute(graph.getGraph());
        logger.info("finished execution - result: {}", result);
    }
}
