package org.ldbcouncil.finbench.impls.gradoop.queries.simple.read1;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead1;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead1Result;
import org.ldbcouncil.finbench.impls.gradoop.CommonUtils;

public class SimpleRead1GradoopOperator implements
    UnaryBaseGraphToValueOperator<TemporalGraph, List<SimpleRead1Result>> {
    private final long id;

    public SimpleRead1GradoopOperator(SimpleRead1 simpleRead1) {
        this.id = simpleRead1.getId();
    }

    @Override
    public List<SimpleRead1Result> execute(TemporalGraph temporalGraph) {
        DataSet<GraphTransaction> accounts = temporalGraph.query("MATCH (a:Account) WHERE a.id = " + id + "L")
            .toGraphCollection()
            .getGraphTransactions();

        DataSet<Tuple3<Date, Boolean, String>> dataSetResult = accounts
            .map(new MapFunction<GraphTransaction, Tuple3<Date, Boolean, String>>() {
                @Override
                public Tuple3<Date, Boolean, String> map(GraphTransaction graphTransaction) throws Exception {
                    Map<String, GradoopId> m = CommonUtils.getVariableMapping(graphTransaction);

                    GradoopId accountId = m.get("a");

                    EPGMVertex account = graphTransaction.getVertexById(accountId);

                    Date createTime =
                        CommonUtils.parseUnixTimeString(account.getPropertyValue("createTime").toString());
                    Boolean isBlocked = account.getPropertyValue("isBlocked").getBoolean();
                    String type = account.getPropertyValue("type").getString();

                    return new Tuple3<>(createTime, isBlocked, type);
                }
            });
        List<SimpleRead1Result> simpleRead1Results = new ArrayList<>();
        try {
            dataSetResult.collect().forEach(
                tuple -> simpleRead1Results.add(new SimpleRead1Result(tuple.f0, tuple.f1, tuple.f2)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return simpleRead1Results;
    }
}
