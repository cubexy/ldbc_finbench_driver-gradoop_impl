package org.ldbcouncil.finbench.impls.gradoop.queries.simple.read2;

import java.util.List;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead2;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead2Result;
import org.ldbcouncil.finbench.impls.gradoop.FlinkCmdArg;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.queries.simple.AbstractCmdArgExecutor;

public class SimpleRead2CmdArgExecutor extends AbstractCmdArgExecutor<List<SimpleRead2Result>> {

    private SimpleRead2GradoopOperator operator;

    @Override
    public String getQueryTitle() {
        return "SimpleRead2";
    }

    @Override
    public String getQueryKey() {
        return "simple_read_2";
    }

    @Override
    public List<SimpleRead2Result> executeQuery(GradoopFinbenchBaseGraphState graph) throws DbException {
        return operator.execute(graph.getGraph());
    }

    @Override
    public void init(FlinkCmdArg inputArgs) {
        SimpleRead2 input = new SimpleRead2(inputArgs.getId(), inputArgs.getStartTime(), inputArgs.getEndTime());
        this.operator = new SimpleRead2GradoopOperator(input);
    }
}
