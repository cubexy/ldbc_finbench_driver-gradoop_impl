package org.ldbcouncil.finbench.impls.gradoop.queries.simple.read1;

import java.util.List;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead1;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead1Result;
import org.ldbcouncil.finbench.impls.gradoop.FlinkCmdArg;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.queries.AbstractCmdArgExecutor;

public class SimpleRead1CmdArgExecutor extends AbstractCmdArgExecutor<List<SimpleRead1Result>> {

    private SimpleRead1GradoopOperator operator;

    @Override
    public String getQueryTitle() {
        return "SimpleRead1";
    }

    @Override
    public String getQueryKey() {
        return "simple_read_1";
    }

    @Override
    public List<SimpleRead1Result> executeQuery(GradoopFinbenchBaseGraphState graph) {
        return operator.execute(graph.getGraph());
    }

    @Override
    public void init(FlinkCmdArg inputArgs) {
        SimpleRead1 input = new SimpleRead1(inputArgs.getId(), inputArgs.getStartTime(), inputArgs.getEndTime());
        this.operator = new SimpleRead1GradoopOperator(input);
    }
}
