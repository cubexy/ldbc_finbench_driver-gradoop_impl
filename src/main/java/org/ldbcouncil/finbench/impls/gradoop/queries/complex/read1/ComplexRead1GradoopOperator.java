package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read1;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.functions.epgm.LabelIsIn;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead1;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead1Result;
import org.ldbcouncil.finbench.impls.gradoop.CommonUtils;

class ComplexRead1GradoopOperator implements
    UnaryBaseGraphToValueOperator<TemporalGraph, List<ComplexRead1Result>> {

    private final long id;
    private final long startTime;
    private final long endTime;
    private final int truncationLimit;
    private final boolean isTruncationOrderAscending;

    public ComplexRead1GradoopOperator(ComplexRead1 complexRead1) {
        this.id = complexRead1.getId();
        this.startTime = complexRead1.getStartTime().getTime();
        this.endTime = complexRead1.getEndTime().getTime();
        this.truncationLimit = complexRead1.getTruncationLimit();
        final TruncationOrder truncationOrder = complexRead1.getTruncationOrder();
        this.isTruncationOrderAscending = truncationOrder == TruncationOrder.TIMESTAMP_ASCENDING;
    }

    @Override
    public List<ComplexRead1Result> execute(TemporalGraph temporalGraph) {
        TemporalGraph windowedGraph = temporalGraph
            .subgraph(new LabelIsIn<>("Account", "Medium"), new LabelIsIn<>("transfer", "signIn"))
            .fromTo(this.startTime, this.endTime);

        DataSet<GraphTransaction> gtxLength3 = windowedGraph
            .temporalQuery("MATCH (a:Account)-[t1:transfer]->(:Account)-[t2:transfer]->(:Account)" +
                "-[t3:transfer]->" +
                "(other:Account)<-[s:signIn]-(m:Medium)" +
                " WHERE a.id = " + this.id + "L AND t1.val_from.before(t2.val_from) AND t2.val_from.before(t3" +
                ".val_from) AND m.isBlocked = true")
            .toGraphCollection()
            .getGraphTransactions();
        // sort with sortPartition and limit

        DataSet<GraphTransaction> gtxLength2 = windowedGraph
            .temporalQuery(
                "MATCH (a:Account)-[t1:transfer]->(:Account)-[t2:transfer]->(other:Account)<-[s:signIn]-(m:Medium)" +
                    " WHERE a.id = " + this.id + "L AND t1.val_from.before(t2.val_from) AND m.isBlocked = true")
            .toGraphCollection()
            .getGraphTransactions();

        DataSet<GraphTransaction> gtxLength1 = windowedGraph
            .temporalQuery("MATCH (a:Account)-[t1:transfer]->(other:Account)<-[s:signIn]-(m:Medium)" +
                " WHERE a.id = " + this.id + "L AND m.isBlocked = true")
            .toGraphCollection()
            .getGraphTransactions();

        DataSet<Tuple4<Long, Integer, Long, String>> result =
            gtxLength1.union(gtxLength2).union(gtxLength3)
                .map(new MapFunction<GraphTransaction, Tuple4<Long, Integer, Long, String>>() {
                    @Override
                    public Tuple4<Long, Integer, Long, String> map(GraphTransaction graphTransaction) throws Exception {
                        Map<String, GradoopId> m = CommonUtils.getVariableMapping(graphTransaction);

                        GradoopId otherGradoopId = m.get("other");
                        GradoopId mediumGradoopId = m.get("m");

                        Long otherId =
                            graphTransaction.getVertexById(otherGradoopId).getPropertyValue("id").getLong();
                        int accountDistance = graphTransaction.getEdges().size() - 1;
                        Long mediumId =
                            graphTransaction.getVertexById(mediumGradoopId).getPropertyValue("id").getLong();
                        String mediumType =
                            graphTransaction.getVertexById(mediumGradoopId).getPropertyValue("type").getString();
                        return new Tuple4<>(otherId, accountDistance, mediumId,
                            mediumType);
                    }
                }).distinct(0, 1, 2, 3);

        result = result
            .sortPartition(1, Order.ASCENDING)
            .sortPartition(0, Order.ASCENDING)
            .sortPartition(3, Order.ASCENDING);

        List<ComplexRead1Result> complexRead1Results = new ArrayList<>();

        try {
            result.collect().forEach(
                tuple -> complexRead1Results.add(new ComplexRead1Result(tuple.f0, tuple.f1, tuple.f2, tuple.f3)));
        } catch (Exception e) {
            throw new RuntimeException("Error while collecting results for complex read 1: " + e);
        }

        return complexRead1Results;
    }
}
