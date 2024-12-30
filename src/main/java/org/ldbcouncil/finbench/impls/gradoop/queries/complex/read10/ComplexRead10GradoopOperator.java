package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read10;

import java.util.Collections;
import java.util.List;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead10;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead10Result;

class ComplexRead10GradoopOperator implements UnaryBaseGraphToValueOperator<TemporalGraph, List<ComplexRead10Result>> {

    private final long id;
    private final long id2;
    private final long startTime;
    private final long endTime;

    public ComplexRead10GradoopOperator(ComplexRead10 cr10) {
        this.id = cr10.getPid1();
        this.id2 = cr10.getPid2();
        this.startTime = cr10.getStartTime().getTime();
        this.endTime = cr10.getEndTime().getTime();
    }

    @Override
    public List<ComplexRead10Result> execute(TemporalGraph temporalGraph) {
        return Collections.singletonList(new ComplexRead10Result(0));
    }
}
