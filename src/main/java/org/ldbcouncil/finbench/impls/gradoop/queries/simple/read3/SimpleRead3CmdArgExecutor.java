package org.ldbcouncil.finbench.impls.gradoop.queries.simple.read3;

import java.util.List;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead3;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead3Result;
import org.ldbcouncil.finbench.impls.gradoop.FlinkCmdArg;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.queries.simple.AbstractCmdArgExecutor;

public class SimpleRead3CmdArgExecutor extends AbstractCmdArgExecutor<List<SimpleRead3Result>> {

    private SimpleRead3GradoopOperator operator;

    @Override
    public String getQueryTitle() {
        return "SimpleRead3";
    }

    @Override
    public String getQueryKey() {
        return "simple_read_3";
    }

    @Override
    public List<SimpleRead3Result> executeQuery(GradoopFinbenchBaseGraphState graph) throws DbException {
        return operator.execute(graph.getGraph());
    }

    @Override
    public void init(FlinkCmdArg inputArgs) {
        SimpleRead3 input = new SimpleRead3(inputArgs.getId(), inputArgs.getThreshold(), inputArgs.getStartTime(), inputArgs.getEndTime());
        this.operator = new SimpleRead3GradoopOperator(input);
    }
}
