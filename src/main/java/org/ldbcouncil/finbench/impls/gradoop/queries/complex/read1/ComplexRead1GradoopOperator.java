package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read1;

import java.util.ArrayList;
import java.util.Comparator;
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
    private final boolean useFlinkSort;

    public ComplexRead1GradoopOperator(ComplexRead1 complexRead1, boolean useFlinkSort) {
        this.id = complexRead1.getId();
        this.startTime = complexRead1.getStartTime().getTime();
        this.endTime = complexRead1.getEndTime().getTime();
        this.truncationLimit = complexRead1.getTruncationLimit();
        final TruncationOrder truncationOrder = complexRead1.getTruncationOrder();
        this.isTruncationOrderAscending = truncationOrder == TruncationOrder.TIMESTAMP_ASCENDING;
        this.useFlinkSort = useFlinkSort;
    }

    /**
     * Given an Account and a specified time window between startTime and endTime, find all the Account
     * that is signed in by a blocked Medium and has fund transferred via edge1 by at most 3 steps. Note
     * that all timestamps in the transfer trace must be in ascending order(only greater than). Return
     * the id of the account, the distance from the account to given one, the id and type of the related
     * medium.
     * Note: The returned accounts may exist in different distance from the given one.
     *
     * @param temporalGraph input graph
     * @return id of the account, the distance from the account to given one, the id and type of the related
     * medium
     */
    @Override
    public List<ComplexRead1Result> execute(TemporalGraph temporalGraph) {
        // TODO: implement truncation strategy
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

        DataSet<Tuple4<Long, Integer, Long, String>> mapResult =
            gtxLength1.union(gtxLength2).union(gtxLength3)
                .map(new MapFunction<GraphTransaction, Tuple4<Long, Integer, Long, String>>() {
                    @Override
                    public Tuple4<Long, Integer, Long, String> map(GraphTransaction graphTransaction) {
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

        if (this.useFlinkSort) {
            windowedGraph.getConfig().getExecutionEnvironment().setParallelism(1);

            mapResult = mapResult
                .sortPartition(1, Order.ASCENDING)
                .sortPartition(0, Order.ASCENDING)
                .sortPartition(3, Order.ASCENDING);
        }

        List<Tuple4<Long, Integer, Long, String>> resultList;

        try {
            resultList = mapResult.collect();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (!this.useFlinkSort) {
            resultList.sort(Comparator
                .comparing((Tuple4<Long, Integer, Long, String> t) -> t.f1)
                .thenComparing(t -> t.f0)
                .thenComparing(t -> t.f3));
        }

        List<ComplexRead1Result> complexRead1Results = new ArrayList<>();

        for (Tuple4<Long, Integer, Long, String> result : resultList) {
            complexRead1Results.add(new ComplexRead1Result(result.f0, result.f1, result.f2, result.f3));
        }

        return complexRead1Results;
    }
}
