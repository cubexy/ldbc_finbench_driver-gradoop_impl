package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read11;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.functions.bool.False;
import org.gradoop.flink.model.impl.functions.epgm.LabelIsIn;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.Count;
import org.gradoop.flink.model.impl.operators.aggregation.functions.max.MaxProperty;
import org.gradoop.flink.model.impl.operators.aggregation.functions.sum.SumProperty;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.model.impl.operators.keyedgrouping.GroupingKeys;
import org.gradoop.flink.model.impl.operators.keyedgrouping.KeyedGrouping;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead11;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead11Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead1Result;
import org.ldbcouncil.finbench.impls.gradoop.CommonUtils;

class ComplexRead11GradoopOperator implements
    UnaryBaseGraphToValueOperator<TemporalGraph, List<ComplexRead11Result>> {

    private final long id;
    private final long startTime;
    private final long endTime;
    private final int truncationLimit;
    private final boolean isTruncationOrderAscending;

    public ComplexRead11GradoopOperator(ComplexRead11 cr11) {
        this.id = cr11.getId();
        this.startTime = cr11.getStartTime().getTime();
        this.endTime = cr11.getEndTime().getTime();
        this.truncationLimit = cr11.getTruncationLimit();
        final TruncationOrder truncationOrder = cr11.getTruncationOrder();
        this.isTruncationOrderAscending = truncationOrder == TruncationOrder.TIMESTAMP_ASCENDING;
    }


    /**
     * Executes the complex read 11 query.
     * @implNote Implementation only supports path lengths of up to 5 Person nodes because Gradoop does not support
     * variable length paths.
     */
    @Override
    public List<ComplexRead11Result> execute(TemporalGraph temporalGraph) {
        // TODO: implement truncation strategy
        TemporalGraph windowedGraph = temporalGraph
            .subgraph(new LabelIsIn<>("Person", "Loan"), new LabelIsIn<>("guarantee", "apply"))
            .fromTo(this.startTime, this.endTime);

        DataSet<Tuple2<Double, Integer>> loanEdges = windowedGraph
            .temporalQuery(
                "MATCH (p1:Person)-[:guarantee]->(p2:Person)-[:guarantee]->(p3:Person)-[:guarantee]->(p4:Person)-[:guarantee]->(p5:Person), (p2)-[:apply]->(:Loan), (p3)-[:apply]->(:Loan), (p4)-[:apply]->(:Loan), (p5)-[:apply]->(:Loan) " +
                    " WHERE p1.id = " + this.id + "L"
            )
            .union(
                windowedGraph.temporalQuery(
                "MATCH (p1:Person)-[:guarantee]->(p2:Person)-[:guarantee]->(p3:Person)-[:guarantee]->(p4:Person), (p2)-[:apply]->(:Loan), (p3)-[:apply]->(:Loan), (p4)-[:apply]->(:Loan) " +
                    " WHERE p1.id = " + this.id + "L"
                )
            )
            .union(
                windowedGraph
                    .temporalQuery(
                        "MATCH (p1:Person)-[:guarantee]->(p2:Person)-[:guarantee]->(p3:Person), (p2)-[:apply]->(:Loan), (p3)-[:apply]->(:Loan) " +
                            " WHERE p1.id = " + this.id + "L")
            )
            .union(
                windowedGraph
                    .temporalQuery("MATCH (p1:Person)-[:guarantee]->(p2:Person)-[:apply]->(:Loan)" +
                        " WHERE p1.id = " + this.id + "L")
            )
            .reduce(new ReduceCombination<>())
            .query("MATCH (:Loan)")
            .reduce(new ReduceCombination<>())
            .transformVertices((currentVertex, transformedVertex) -> {
                if (currentVertex.hasProperty("id")) {
                    currentVertex.removeProperty("id");
                }
                return currentVertex;
            })
            .callForGraph(
                new KeyedGrouping<>(Arrays.asList(GroupingKeys.label(), GroupingKeys.property("id")),
                    Arrays.asList(new Count("count"), new SumProperty("loanAmount")),
                    null,
                    null
                )
            ).getVertices()
            .map(new MapFunction<TemporalVertex, Tuple2<Double, Integer>>() {
                @Override
                public Tuple2<Double, Integer> map(TemporalVertex temporalVertex) throws Exception {
                    double sumAmount = temporalVertex.getPropertyValue("sum_loanAmount").getDouble();
                    int count = (int) temporalVertex.getPropertyValue("count").getLong();
                    return new Tuple2<>(sumAmount, count);
                }
            });


        List<Tuple2<Double, Integer>> loanEdgesList;

        try {
            loanEdgesList = loanEdges.collect();
        } catch (Exception e) {
            throw new RuntimeException("Error while collecting results for complex read 11: " + e);
        }

        if (loanEdgesList.isEmpty()) {
            return Collections.singletonList(new ComplexRead11Result(0.0f, 0));
        }

        return Collections.singletonList(new ComplexRead11Result(loanEdgesList.get(0).f0, loanEdgesList.get(0).f1));
    }
}
