package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read10;

import java.util.List;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead10;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead10Result;
import org.ldbcouncil.finbench.impls.gradoop.FlinkCmdArg;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.queries.AbstractCmdArgExecutor;

public class ComplexRead10CmdArgExecutor extends AbstractCmdArgExecutor<List<ComplexRead10Result>> {

    private ComplexRead10GradoopOperator operator;

    @Override
    public String getQueryTitle() {
        return "ComplexRead10";
    }

    @Override
    public String getQueryKey() {
        return "complex_read_10";
    }

    @Override
    public List<ComplexRead10Result> executeQuery(GradoopFinbenchBaseGraphState graph) throws DbException {
        return operator.execute(graph.getGraph());
    }

    @Override
    public void init(FlinkCmdArg inputArgs) {
        ComplexRead10 input = new ComplexRead10(inputArgs.getPersonId(), inputArgs.getPersonId2(), inputArgs.getStartTime(), inputArgs.getEndTime());
        this.operator = new ComplexRead10GradoopOperator(input);
    }
}
