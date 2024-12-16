package org.ldbcouncil.finbench.impls.gradoop.queries.simple.read6;

import static org.ldbcouncil.finbench.impls.gradoop.CommonUtils.roundToDecimalPlaces;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.functions.epgm.LabelIsIn;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead6;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead6Result;
import org.ldbcouncil.finbench.impls.gradoop.CommonUtils;

class SimpleRead6GradoopOperator implements UnaryBaseGraphToValueOperator<TemporalGraph, List<SimpleRead6Result>> {

    private final Long id;
    private final Date startTime;
    private final Date endTime;

    public SimpleRead6GradoopOperator(SimpleRead6 sr6) {
        this.id = sr6.getId();
        this.startTime = sr6.getStartTime();
        this.endTime = sr6.getEndTime();
    }

    @Override
    public List<SimpleRead6Result> execute(TemporalGraph temporalGraph) {
        TemporalGraph windowedGraph = temporalGraph
            .subgraph(new LabelIsIn<>("Account"), new LabelIsIn<>("transfer"))
            .fromTo(this.startTime.getTime(), this.endTime.getTime());

            DataSet<GraphTransaction> accounts = windowedGraph.query(
                    "MATCH (src:Account)<-[e1:transfer]-(mid:Account)-[e2:transfer]->(dst:Account) WHERE src <> dst AND src.id =" +
                        this.id +
                        "L AND dst.isBlocked = true")
                .toGraphCollection()
                .getGraphTransactions();

        DataSet<Tuple1<Long>> dataSetResult = accounts
            .map(new MapFunction<GraphTransaction, Tuple1<Long>>() {
                @Override
                public Tuple1<Long> map(GraphTransaction graphTransaction) throws Exception {
                    Map<String, GradoopId> m = CommonUtils.getVariableMapping(graphTransaction);

                    GradoopId accountId = m.get("a");

                    EPGMVertex account = graphTransaction.getVertexById(accountId);

                    Long id = account.getPropertyValue("id").getLong();

                    return new Tuple1<>(id);
                }
            })
            .distinct(0);

        windowedGraph.getConfig().getExecutionEnvironment().setParallelism(1);

        dataSetResult = dataSetResult
            .sortPartition(0, Order.ASCENDING);

        List<SimpleRead6Result> simpleRead6Results = new ArrayList<>();

        try {
            dataSetResult.collect().forEach(
                tuple -> simpleRead6Results.add(new SimpleRead6Result(tuple.f0)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return simpleRead6Results;
    }
}
