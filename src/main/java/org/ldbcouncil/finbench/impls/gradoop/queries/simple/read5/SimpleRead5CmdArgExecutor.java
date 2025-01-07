package org.ldbcouncil.finbench.impls.gradoop.queries.simple.read5;

import java.util.List;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead5;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead5Result;
import org.ldbcouncil.finbench.impls.gradoop.FlinkCmdArg;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.queries.AbstractCmdArgExecutor;

public class SimpleRead5CmdArgExecutor extends AbstractCmdArgExecutor<List<SimpleRead5Result>> {

    private SimpleRead5GradoopOperator operator;

    @Override
    public String getQueryTitle() {
        return "SimpleRead5";
    }

    @Override
    public String getQueryKey() {
        return "simple_read_5";
    }

    @Override
    public List<SimpleRead5Result> executeQuery(GradoopFinbenchBaseGraphState graph) throws DbException {
        return operator.execute(graph.getGraph());
    }

    @Override
    public void init(FlinkCmdArg inputArgs) {
        SimpleRead5 input = new SimpleRead5(inputArgs.getId(), inputArgs.getThreshold(), inputArgs.getStartTime(),
            inputArgs.getEndTime());
        this.operator = new SimpleRead5GradoopOperator(input, inputArgs.isClusterSort());
    }
}
