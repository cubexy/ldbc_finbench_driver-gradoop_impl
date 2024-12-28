package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read6;

import java.util.Date;
import java.util.List;
import java.util.Objects;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.functions.epgm.LabelIsIn;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead6;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead6Result;

class ComplexRead6GradoopOperator implements
    UnaryBaseGraphToValueOperator<TemporalGraph, List<ComplexRead6Result>> {

    private final long id;
    private final long startTime;
    private final long endTime;
    private final int truncationLimit;
    private final boolean isTruncationOrderAscending;
    private final double threshold1;
    private final double threshold2;

    public ComplexRead6GradoopOperator(ComplexRead6 cr6) {
        this.id = cr6.getId();
        this.startTime = cr6.getStartTime().getTime();
        this.endTime = cr6.getEndTime().getTime();
        this.truncationLimit = cr6.getTruncationLimit();
        final TruncationOrder truncationOrder = cr6.getTruncationOrder();
        this.isTruncationOrderAscending = truncationOrder == TruncationOrder.TIMESTAMP_ASCENDING;
        this.threshold1 = cr6.getThreshold1();
        this.threshold2 = cr6.getThreshold2();
    }

    @Override
    public List<ComplexRead6Result> execute(TemporalGraph temporalGraph) {
        final long id_serializable = this.id;

        // TODO: implement truncation strategy
        TemporalGraph windowedGraph = temporalGraph
            .subgraph(new LabelIsIn<>("Account"), new LabelIsIn<>("transfer", "withdraw"))
            .fromTo(this.startTime, this.endTime);

        windowedGraph.query(
            "MATCH (src1:Account)-[edge1:transfer]->(mid:Account)-[edge2:withdraw]->(dstCard:Account) WHERE dstCard.id = " +
                this.id + "L AND dstCard.type = 'card' AND edge1.amount > " + this.threshold1 + " AND edge2.amount > " + this.threshold2)
            .reduce(new ReduceCombination<>())
            .transformVertices((currentVertex, transformedVertex) -> {
                // note: this only removed the id of dstCard, not id of src1. therefore, grouping only by mid id is not possible since
                // we cannot find out if the current edge is src1 or mid.
                // we cannot access outgoing edges, otherwise we could say "do you have an outgoing withdraw edge?"
                // we cannot access variable mappings, otherwise we could say "is your label "mid"?"

                // therefore, we can group by mid id, but we have to aggregate src1 later
                if (currentVertex.hasProperty("id") && (
                    Objects.equals(currentVertex.getPropertyValue("id").getLong(), id_serializable)
                )) {
                    currentVertex.removeProperty("id");
                }
                return currentVertex;
            });

        return null;
    }
}
