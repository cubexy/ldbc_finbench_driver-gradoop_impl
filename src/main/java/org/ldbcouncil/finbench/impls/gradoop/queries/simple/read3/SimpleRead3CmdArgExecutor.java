package org.ldbcouncil.finbench.impls.gradoop.queries.simple.read3;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.logging.log4j.Logger;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead3;
import org.ldbcouncil.finbench.impls.gradoop.FlinkCmdArg;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;

public class SimpleRead3CmdArgExecutor {
    public static void execute(FlinkCmdArg inputArgs, GradoopFinbenchBaseGraphState graph, Logger logger) throws DbException {
        logger.info("executing query SimpleRead3 with args {}", inputArgs);
        SimpleRead3 input = new SimpleRead3(inputArgs.getId(), inputArgs.getThreshold(), inputArgs.getStartTime(), inputArgs.getEndTime());
        SimpleRead3GradoopOperator operator = new SimpleRead3GradoopOperator(input);
        logger.info("started execution...");
        Tuple1<Float> result = operator.execute(graph.getGraph());
        logger.info("finished execution - result: {}", result);
    }
}
