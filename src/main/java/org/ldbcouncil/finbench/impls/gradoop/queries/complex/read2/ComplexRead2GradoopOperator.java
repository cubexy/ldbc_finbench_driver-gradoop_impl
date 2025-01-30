package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read2;

import static org.ldbcouncil.finbench.impls.gradoop.CommonUtils.roundToDecimalPlaces;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.functions.epgm.LabelIsIn;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
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
    private final boolean useFlinkSort;

    public ComplexRead2GradoopOperator(ComplexRead2 complexRead2, boolean useFlinkSort) {
        this.id = complexRead2.getId();
        this.startTime = complexRead2.getStartTime().getTime();
        this.endTime = complexRead2.getEndTime().getTime();
        this.truncationLimit = complexRead2.getTruncationLimit();
        final TruncationOrder truncationOrder = complexRead2.getTruncationOrder();
        this.isTruncationOrderAscending = truncationOrder == TruncationOrder.TIMESTAMP_ASCENDING;
        this.useFlinkSort = useFlinkSort;
    }

    /**
     * Given a Person and a specified time window between startTime and endTime, find an Account owned
     * by the Person which has fund transferred from other Accounts by at most 3 steps (edge2) which has
     * fund deposited from a loan. The timestamps of in transfer trace (edge2) must be in ascending order
     * (only greater than) from the upstream to downstream. Return the sum of distinct loan amount,
     * the sum of distinct loan balance and the count of distinct loans.
     *
     * @param temporalGraph input graph
     * @return ID of other account, sum of loan amount, sum of loan balance
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
                    " WHERE p.id = " + this.id + "L")
            .toGraphCollection();

        GraphCollection gtxLength2 = windowedGraph
            .temporalQuery(
                "MATCH (p:Person)-[e1:own]->(a:Account)<-[t1:transfer]-(:Account)<-[e2:transfer]-(other:Account)<-[e3:deposit]-(loan:Loan)" +
                    " WHERE p.id = " + this.id + "L AND e2.val_from.before(t1.val_from)")
            .toGraphCollection();

        GraphCollection gtxLength3 = windowedGraph
            .temporalQuery(
                "MATCH (p:Person)-[e1:own]->(a:Account)<-[t2:transfer]-(:Account)<-[t1:transfer]-(:Account)<-[e2:transfer]-(other:Account)<-[e3:deposit]-(loan:Loan)" +
                    " WHERE p.id = " + this.id +
                    "L AND e2.val_from.before(t1.val_from) AND t1.val_from.before(t2.val_from)")
            .toGraphCollection();

        DataSet<GraphTransaction> gtUnion =
            gtxLength1.union(gtxLength2).union(gtxLength3).reduce(new ReduceCombination<>())
                .query("MATCH (other:Account)<-[e3:deposit]-(loan:Loan)") // get last part of unioned query
                .getGraphTransactions();

        DataSet<Tuple4<Long, Double, Double, Long>>
            edgeMap = gtUnion.map(new MapFunction<GraphTransaction, Tuple4<Long, Double, Double, Long>>() {
                @Override
                public Tuple4<Long, Double, Double, Long> map(GraphTransaction graphTransaction) {
                    Map<String, GradoopId> m = CommonUtils.getVariableMapping(graphTransaction);
                    EPGMVertex other = graphTransaction.getVertexById(m.get("other"));
                    EPGMVertex loan = graphTransaction.getVertexById(m.get("loan"));

                    long otherID = other.getPropertyValue("id").getLong();
                    double loanAmount = loan.getPropertyValue("loanAmount").getDouble();
                    double loanBalance = loan.getPropertyValue("balance").getDouble();
                    long loanID = loan.getPropertyValue("id").getLong();
                    return new Tuple4<>(otherID, loanAmount, loanBalance, loanID);
                }

            }).distinct(0, 3)
            .groupBy(0)
            .reduce(new ReduceFunction<Tuple4<Long, Double, Double, Long>>() {
                @Override
                public Tuple4<Long, Double, Double, Long> reduce(Tuple4<Long, Double, Double, Long> t1,
                                                                 Tuple4<Long, Double, Double, Long> t2) {
                    return new Tuple4<>(t1.f0, t1.f1 + t2.f1, t1.f2 + t2.f2, 0L);
                }
            });

        if (this.useFlinkSort) {
            windowedGraph.getConfig().getExecutionEnvironment().setParallelism(1);

            edgeMap = edgeMap
                .sortPartition(1, Order.DESCENDING)
                .sortPartition(0, Order.ASCENDING);
        }

        List<Tuple4<Long, Double, Double, Long>> edges;
        try {
            edges = edgeMap.collect();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (!this.useFlinkSort) {
            edges.sort(Comparator
                .comparing((Tuple4<Long, Double, Double, Long> t) -> t.f1, Comparator.reverseOrder())
                .thenComparing(t -> t.f0));
        }

        List<ComplexRead2Result> complexRead2Results = new ArrayList<>();

        for (Tuple4<Long, Double, Double, Long> edge : edges) {
            complexRead2Results.add(
                new ComplexRead2Result(edge.f0, roundToDecimalPlaces(edge.f1, 3), roundToDecimalPlaces(edge.f2, 3)));
        }

        return complexRead2Results;
    }
}
