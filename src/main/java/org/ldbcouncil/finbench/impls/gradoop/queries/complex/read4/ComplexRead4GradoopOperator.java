package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read4;

import java.util.Date;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple7;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead4;

class ComplexRead4GradoopOperator implements
    UnaryBaseGraphToValueOperator<TemporalGraph, DataSet<Tuple7<Long, Long, Double, Double, Long, Double, Double>>> {

    private final Date startTime;
    private final Date endTime;
    private final Long id1;
    private final Long id2;

    public ComplexRead4GradoopOperator(ComplexRead4 cr4) {
        this.startTime = cr4.getStartTime();
        this.endTime = cr4.getEndTime();
        this.id1 = cr4.getId1();
        this.id2 = cr4.getId2();
    }

    @Override
    public DataSet<Tuple7<Long, Long, Double, Double, Long, Double, Double>> execute(TemporalGraph temporalGraph) {
        return null;
    }
}
