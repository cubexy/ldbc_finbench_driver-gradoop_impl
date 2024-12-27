package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read5;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.functions.epgm.LabelIsIn;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.result.Path;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead5;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead5Result;
import org.ldbcouncil.finbench.impls.gradoop.CommonUtils;

class ComplexRead5GradoopOperator implements UnaryBaseGraphToValueOperator<TemporalGraph, List<ComplexRead5Result>> {

    private final long id;
    private final long startTime;
    private final long endTime;
    private final int truncationLimit;
    private final boolean isTruncationOrderAscending;

    public ComplexRead5GradoopOperator(ComplexRead5 cr5) {
        this.id = cr5.getId();
        this.startTime = cr5.getStartTime().getTime();
        this.endTime = cr5.getEndTime().getTime();
        this.truncationLimit = cr5.getTruncationLimit();
        final TruncationOrder truncationOrder = cr5.getTruncationOrder();
        this.isTruncationOrderAscending = truncationOrder == TruncationOrder.TIMESTAMP_ASCENDING;
    }

    @Override
    public List<ComplexRead5Result> execute(TemporalGraph temporalGraph) {
        TemporalGraph windowedGraph = temporalGraph
            .subgraph(new LabelIsIn<>("Account", "Person"), new LabelIsIn<>("transfer", "own"))
            .fromTo(this.startTime, this.endTime);

        DataSet<GraphTransaction> gtxLength3 = windowedGraph
            .temporalQuery("MATCH (p:Person)-[:own]->(src:Account)-[edge2:transfer]->(dst1:Account)" +
                "-[edge3:transfer]->(dst2:Account)" +
                "-[edge4:transfer]->(dst3:Account)" +
                " WHERE p.id = " + this.id +
                "L AND edge2.val_from.before(edge3.val_from) AND edge3.val_from.before(edge4" +
                ".val_from) AND src <> dst1 AND src <> dst2 AND src <> dst3 AND dst1 <> dst2 AND dst1 <> dst3 AND dst2 <> dst3")
            .toGraphCollection()
            .getGraphTransactions();

        DataSet<GraphTransaction> gtxLength2 = windowedGraph
            .temporalQuery("MATCH (p:Person)-[:own]->(src:Account)-[edge2:transfer]->(dst1:Account)" +
                "-[edge3:transfer]->(dst2:Account)" +
                " WHERE p.id = " + this.id +
                "L AND edge2.val_from.before(edge3.val_from) AND src <> dst1 AND src <> dst2 AND dst1 <> dst2")
            .toGraphCollection()
            .getGraphTransactions();

        DataSet<GraphTransaction> gtxLength1 = windowedGraph
            .temporalQuery("MATCH (p:Person)-[:own]->(src:Account)-[edge2:transfer]->(dst1:Account)" +
                " WHERE p.id = " + this.id +
                "L AND src <> dst1")
            .toGraphCollection()
            .getGraphTransactions();

        DataSet<Tuple5<Long, Long, Long, Long, Integer>> result = gtxLength1.union(gtxLength2).union(gtxLength3)
            .map(new MapFunction<GraphTransaction, Tuple5<Long, Long, Long, Long, Integer>>() {
                @Override
                public Tuple5<Long, Long, Long, Long, Integer> map(GraphTransaction graphTransaction) throws Exception {
                    Map<String, GradoopId> m = CommonUtils.getVariableMapping(graphTransaction);

                    long srcId = graphTransaction.getVertexById(m.get("src")).getPropertyValue("id").getLong();
                    long dst1Id = graphTransaction.getVertexById(m.get("dst1")).getPropertyValue("id").getLong();
                    int accountDistance = graphTransaction.getEdges().size() - 1;
                    List<Long> path = new ArrayList<>();
                    path.add(srcId);
                    path.add(dst1Id);

                    if (accountDistance > 1) {
                        long dst2Id = graphTransaction.getVertexById(m.get("dst2")).getPropertyValue("id").getLong();
                        path.add(dst2Id);
                        if (accountDistance > 2) {
                            long dst3Id =
                                graphTransaction.getVertexById(m.get("dst3")).getPropertyValue("id").getLong();
                            path.add(dst3Id);

                            return new Tuple5<>(srcId, dst1Id, dst2Id, dst3Id, accountDistance);
                        }

                        return new Tuple5<>(srcId, dst1Id, dst2Id, -1L, accountDistance);
                    }

                    return new Tuple5<>(srcId, dst1Id, -1L, -1L, accountDistance);
                }
            }).distinct(0, 1, 2, 3);

        windowedGraph.getConfig().getExecutionEnvironment().setParallelism(1);

        result = result
            .sortPartition(1, Order.DESCENDING);

        List<ComplexRead5Result> complexRead5Results = new ArrayList<>();

        try {
            result.collect().forEach(
                tuple -> complexRead5Results.add(new ComplexRead5Result(CommonUtils.parsePath(Arrays.asList(tuple.f0, tuple.f1, tuple.f2,
                    tuple.f3)))));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return complexRead5Results;
    }
}

