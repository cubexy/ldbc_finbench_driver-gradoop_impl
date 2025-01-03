package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read2;

import static org.ldbcouncil.finbench.impls.gradoop.CommonUtils.roundToDecimalPlaces;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.LabelIsIn;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.operators.aggregation.functions.sum.SumProperty;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.model.impl.operators.keyedgrouping.GroupingKeys;
import org.gradoop.flink.model.impl.operators.keyedgrouping.KeyedGrouping;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead2;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead2Result;

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

    /**
     * Given a Person and a specified time window between startTime and endTime, find an Account owned
     * by the Person which has fund transferred from other Accounts by at most 3 steps (edge2) which has
     * fund deposited from a loan. The timestamps of in transfer trace (edge2) must be in ascending order
     * (only greater than) from the upstream to downstream. Return the sum of distinct loan amount,
     * the sum of distinct loan balance and the count of distinct loans.
     *
     * @param temporalGraph input graph
     * @return sum of distinct loan amount, the sum of distinct loan balance and the count of distinct loans
     */
    @Override
    public List<ComplexRead2Result> execute(TemporalGraph temporalGraph) {
        // TODO: implement truncation strategy
        TemporalGraph windowedGraph = temporalGraph
            .subgraph(new LabelIsIn<>("Account", "Loan", "Person"), new LabelIsIn<>("transfer", "own", "deposit"))
            .fromTo(this.startTime, this.endTime);

        GraphCollection gtxLength1 = windowedGraph
            .temporalQuery(
                "MATCH (p:Person)-[e1:own]->(a:Account)<-[e2:transfer]-(other:Account)<-[e3:deposit]-(loan:Loan)" +
                    " WHERE p.id = " + this.id + "L AND a <> other")
            .toGraphCollection();

        GraphCollection gtxLength2 = windowedGraph
            .temporalQuery(
                "MATCH (p:Person)-[e1:own]->(a:Account)<-[t1:transfer]-(:Account)<-[e2:transfer]-(other:Account)<-[e3:deposit]-(loan:Loan)" +
                    " WHERE p.id = " + this.id + "L AND e2.val_from.before(t1.val_from) AND a <> other")
            .toGraphCollection();

        GraphCollection gtxLength3 = windowedGraph
            .temporalQuery(
                "MATCH (p:Person)-[e1:own]->(a:Account)<-[t2:transfer]-(:Account)<-[t1:transfer]-(:Account)<-[e2:transfer]-(other:Account)<-[e3:deposit]-(loan:Loan)" +
                    " WHERE p.id = " + this.id +
                    "L AND e2.val_from.before(t1.val_from) AND t1.val_from.before(t2.val_from) AND a <> other")
            .toGraphCollection();

        LogicalGraph gcUnion = gtxLength1.union(gtxLength2).union(gtxLength3).reduce(new ReduceCombination<>())
            .query("MATCH (other:Account)<-[e3:deposit]-(loan:Loan)")
            .reduce(new ReduceCombination<>())
            .transformVertices((currentVertex, transformedVertex) -> {
                if (!currentVertex.getLabel().equals("Loan")) {
                    currentVertex.removeProperty("id");
                }
                return currentVertex;
            })
            .callForGraph(
                new KeyedGrouping<>(Arrays.asList(GroupingKeys.label(), GroupingKeys.property("id")),
                    Arrays.asList(new SumProperty("loanAmount"), new SumProperty("balance")), null,
                    null)
            );

        MapOperator<Tuple2<EPGMEdge, EPGMVertex>, Tuple3<Long, Double, Double>>
            edgeMap = gcUnion.getEdges().join(gcUnion.getVertices()).where(new SourceId<>()).equalTo(new Id<>())
            .map(new MapFunction<Tuple2<EPGMEdge, EPGMVertex>, Tuple3<Long, Double, Double>>() {
                @Override
                public Tuple3<Long, Double, Double> map(Tuple2<EPGMEdge, EPGMVertex> e) {
                    EPGMVertex src = e.f1;

                    long otherID = src.getPropertyValue("id").getLong();
                    double loanBalance = src.getPropertyValue("sum_balance").getDouble();
                    double loanAmount = src.getPropertyValue("sum_loanAmount").getDouble();
                    return new Tuple3<>(otherID, loanBalance, loanAmount);
                }

            });

        windowedGraph.getConfig().getExecutionEnvironment().setParallelism(1);


        List<Tuple3<Long, Double, Double>> edges;
        try {
            edges = edgeMap
                .sortPartition(1, Order.DESCENDING)
                .sortPartition(0, Order.ASCENDING)
                .collect();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        List<ComplexRead2Result> complexRead2Results = new ArrayList<>();

        for (Tuple3<Long, Double, Double> edge : edges) {
            complexRead2Results.add(
                new ComplexRead2Result(edge.f0, roundToDecimalPlaces(edge.f1, 3), roundToDecimalPlaces(edge.f2, 3)));
        }

        return complexRead2Results;
    }
}
