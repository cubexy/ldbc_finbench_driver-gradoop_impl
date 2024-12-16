package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.LabelIsIn;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.Count;
import org.gradoop.flink.model.impl.operators.aggregation.functions.max.MaxProperty;
import org.gradoop.flink.model.impl.operators.aggregation.functions.sum.SumProperty;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.model.impl.operators.keyedgrouping.GroupingKeys;
import org.gradoop.flink.model.impl.operators.keyedgrouping.KeyedGrouping;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead2;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead2Result;
import org.ldbcouncil.finbench.impls.gradoop.CommonUtils;

class ComplexRead2GradoopOperator implements
    UnaryBaseGraphToValueOperator<TemporalGraph, List<ComplexRead2Result>> {

    private final long id;
    private final long startTime;
    private final long endTime;
    private final int truncationLimit;
    private final boolean isTruncationOrderAscending;

    public ComplexRead2GradoopOperator(ComplexRead2 complexRead2) {
        this.id = complexRead2.getId();
        this.startTime = complexRead2.getStartTime().getTime();
        this.endTime = complexRead2.getEndTime().getTime();
        this.truncationLimit = complexRead2.getTruncationLimit();
        final TruncationOrder truncationOrder = complexRead2.getTruncationOrder();
        this.isTruncationOrderAscending = truncationOrder == TruncationOrder.TIMESTAMP_ASCENDING;
    }

    @Override
    public List<ComplexRead2Result> execute(TemporalGraph temporalGraph) {
        // TODO: implement truncation strategy
        TemporalGraph windowedGraph = temporalGraph
            .subgraph(new LabelIsIn<>("Account", "Loan", "Person"), new LabelIsIn<>("transfer", "own", "deposit"))
            .fromTo(this.startTime, this.endTime);

        GraphCollection gtxLength1 = windowedGraph
            .temporalQuery(
                "MATCH (p:Person)-[e1:own]->(a:Account)<-[e2:transfer]-(other:Account)<-[e3:deposit]-(loan:Loan)" +
                    " WHERE p.id = " + this.id + "L ")
            .toGraphCollection();

        GraphCollection gtxLength2 = windowedGraph
            .temporalQuery(
                "MATCH (p:Person)-[e1:own]->(a:Account)<-[t1:transfer]-(:Account)<-[e2:transfer]-(other:Account)<-[e3:deposit]-(loan:Loan)" +
                    " WHERE p.id = " + this.id + "L AND e2.val_from.before(t1.val_from)")
            .toGraphCollection();

        GraphCollection gtxLength3 = windowedGraph
            .temporalQuery(
                "MATCH (p:Person)-[e1:own]->(a:Account)<-[t2:transfer]-(:Account)<-[t1:transfer]-(:Account)<-[e2:transfer]-(other:Account)<-[e3:deposit]-(loan:Loan)" +
                    " WHERE p.id = " + this.id + "L AND e2.val_from.before(t1.val_from) AND t1.val_from.before(t2.val_from)")
            .toGraphCollection();

        return null;
    }
}
