package org.ldbcouncil.finbench.impls.gradoop.queries.simple.read6;

import java.util.List;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead6;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead6Result;
import org.ldbcouncil.finbench.impls.gradoop.FlinkCmdArg;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.queries.AbstractCmdArgExecutor;

public class SimpleRead6CmdArgExecutor extends AbstractCmdArgExecutor<List<SimpleRead6Result>> {

    private SimpleRead6GradoopOperator operator;

    @Override
    public String getQueryTitle() {
        return "SimpleRead6";
    }

    @Override
    public String getQueryKey() {
        return "simple_read_6";
    }

    @Override
    public List<SimpleRead6Result> executeQuery(GradoopFinbenchBaseGraphState graph) throws DbException {
        return operator.execute(graph.getGraph());
    }

    @Override
    public void init(FlinkCmdArg inputArgs) {
        SimpleRead6 input = new SimpleRead6(inputArgs.getId(), inputArgs.getStartTime(), inputArgs.getEndTime());
        this.operator = new SimpleRead6GradoopOperator(input, inputArgs.isClusterSort());
    }
}
