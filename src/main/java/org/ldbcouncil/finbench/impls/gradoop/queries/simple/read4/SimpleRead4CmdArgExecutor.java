package org.ldbcouncil.finbench.impls.gradoop.queries.simple.read4;

import java.util.List;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead4;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead4Result;
import org.ldbcouncil.finbench.impls.gradoop.FlinkCmdArg;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.queries.AbstractCmdArgExecutor;

public class SimpleRead4CmdArgExecutor extends AbstractCmdArgExecutor<List<SimpleRead4Result>> {

    private SimpleRead4GradoopOperator operator;

    @Override
    public String getQueryTitle() {
        return "SimpleRead4";
    }

    @Override
    public String getQueryKey() {
        return "simple_read_4";
    }

    @Override
    public List<SimpleRead4Result> executeQuery(GradoopFinbenchBaseGraphState graph) throws DbException {
        return operator.execute(graph.getGraph());
    }

    @Override
    public void init(FlinkCmdArg inputArgs) {
        SimpleRead4 input = new SimpleRead4(inputArgs.getId(), inputArgs.getThreshold(), inputArgs.getStartTime(),
            inputArgs.getEndTime());
        this.operator = new SimpleRead4GradoopOperator(input, inputArgs.getParallelism(), inputArgs.isClusterSort());
    }
}
