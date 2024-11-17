package org.ldbcouncil.finbench.impls.gradoop.queries.simple.read1;

import java.util.Date;
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
import org.ldbcouncil.finbench.impls.gradoop.CommonUtils;

class SimpleRead1GradoopOperator implements
    UnaryBaseGraphToValueOperator<TemporalGraph, DataSet<Tuple3<Date, Boolean, String>>> {
    private final long id;

    public SimpleRead1GradoopOperator(SimpleRead1 simpleRead1) {
        this.id = simpleRead1.getId();
    }

    @Override
    public DataSet<Tuple3<Date, Boolean, String>> execute(TemporalGraph temporalGraph) {
        DataSet<GraphTransaction> accounts = temporalGraph.query("MATCH (a:Account) WHERE a.id = " + id + "L")
            .toGraphCollection()
            .getGraphTransactions();

        return accounts
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

    }
}
