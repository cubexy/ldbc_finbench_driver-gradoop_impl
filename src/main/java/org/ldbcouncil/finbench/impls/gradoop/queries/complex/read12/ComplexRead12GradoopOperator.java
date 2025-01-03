package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read12;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.functions.epgm.LabelIsIn;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.Count;
import org.gradoop.flink.model.impl.operators.aggregation.functions.sum.SumProperty;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.model.impl.operators.keyedgrouping.GroupingKeys;
import org.gradoop.flink.model.impl.operators.keyedgrouping.KeyedGrouping;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead11Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead12;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead12;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead12Result;
import org.ldbcouncil.finbench.impls.gradoop.CommonUtils;

class ComplexRead12GradoopOperator implements
    UnaryBaseGraphToValueOperator<TemporalGraph, List<ComplexRead12Result>> {

    private final long id;
    private final long startTime;
    private final long endTime;
    private final int truncationLimit;
    private final boolean isTruncationOrderAscending;

    public ComplexRead12GradoopOperator(ComplexRead12 cr12) {
        this.id = cr12.getId();
        this.startTime = cr12.getStartTime().getTime();
        this.endTime = cr12.getEndTime().getTime();
        this.truncationLimit = cr12.getTruncationLimit();
        final TruncationOrder truncationOrder = cr12.getTruncationOrder();
        this.isTruncationOrderAscending = truncationOrder == TruncationOrder.TIMESTAMP_ASCENDING;
    }

    @Override
    public List<ComplexRead12Result> execute(TemporalGraph temporalGraph) {
        // TODO: implement truncation strategy
        TemporalGraph windowedGraph = temporalGraph
            .subgraph(new LabelIsIn<>("Person", "Account", "Company"), new LabelIsIn<>("own", "transfer"))
            .fromTo(this.startTime, this.endTime);

        DataSet<Tuple2<Long, Double>> companyAmounts = windowedGraph
            .temporalQuery(
                "MATCH (person:Person)-[edge1:own]->(pAcc:Account)-[edge2:transfer]->(compAcc:Account)<-[edge3:own]-(com:Company)" +
                    " WHERE person.id = " + this.id + "L"
            )
            .toGraphCollection()
            .getGraphTransactions()
            .map(new MapFunction<GraphTransaction, Tuple2<Long, Double>>() {
                @Override
                public Tuple2<Long, Double> map(GraphTransaction graphTransaction) throws Exception {
                    Map<String, GradoopId> m = CommonUtils.getVariableMapping(graphTransaction);
                    GradoopId compAccGradoopId = m.get("compAcc");
                    long compAccId = graphTransaction.getVertexById(compAccGradoopId).getPropertyValue("id").getLong();
                    double amount = 0.0;
                    for (EPGMEdge edge : graphTransaction.getEdges()) {
                        if (edge.getLabel().equals("transfer") && edge.getTargetId().equals(compAccGradoopId)) {
                            amount += edge.getPropertyValue("amount").getDouble();
                        }
                    }

                    return new Tuple2<>(compAccId, amount);
                }
            })
            .groupBy(1)
            .reduce(new ReduceFunction<Tuple2<Long, Double>>() {
                @Override
                public Tuple2<Long, Double> reduce(Tuple2<Long, Double> t1, Tuple2<Long, Double> t2){
                    return new Tuple2<>(t1.f0, t1.f1 + t2.f1);
                }
            });

        windowedGraph.getConfig().getExecutionEnvironment().setParallelism(1);

        companyAmounts = companyAmounts
            .sortPartition(1, Order.DESCENDING)
            .sortPartition(0, Order.ASCENDING);

        List<Tuple2<Long, Double>> loanEdgesList;

        try {
            loanEdgesList = companyAmounts.collect();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        List<ComplexRead12Result> complexRead12Results = new ArrayList<>();

        for (Tuple2<Long, Double> edge : loanEdgesList) {
            complexRead12Results.add(new ComplexRead12Result(edge.f0, CommonUtils.roundToDecimalPlaces(edge.f1, 3)));
        }

        return complexRead12Results;
    }
}
