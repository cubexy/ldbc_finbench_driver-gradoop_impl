package org.ldbcouncil.finbench.impls.gradoop.queries.simple.read2;

import java.util.Date;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.logging.log4j.Logger;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead1;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead2;
import org.ldbcouncil.finbench.impls.gradoop.FlinkCmdArg;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.queries.simple.read1.SimpleRead1GradoopOperator;

public class SimpleRead2CmdArgExecutor {
    public static void execute(FlinkCmdArg inputArgs, GradoopFinbenchBaseGraphState graph, Logger logger) throws DbException {
        logger.info("executing query SimpleRead2 with args {}", inputArgs);
        SimpleRead2 input = new SimpleRead2(inputArgs.getId(), inputArgs.getStartTime(), inputArgs.getEndTime());
        SimpleRead2GradoopOperator operator = new SimpleRead2GradoopOperator(input);
        logger.info("started execution...");
        Tuple6<Double, Double, Long, Double, Double, Long> result = operator.execute(graph.getGraph());
        logger.info("finished execution - result: {}", result);
    }
}
