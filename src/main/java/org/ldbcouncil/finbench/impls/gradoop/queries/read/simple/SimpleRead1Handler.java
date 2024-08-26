package org.ldbcouncil.finbench.impls.gradoop.queries.read.simple;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.functions.bool.True;
import org.gradoop.flink.model.impl.functions.epgm.LabelIsIn;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead1;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead1Result;
import org.ldbcouncil.finbench.impls.gradoop.CommonUtils;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class SimpleRead1Handler implements OperationHandler<SimpleRead1, GradoopFinbenchBaseGraphState> {
    @Override
    public void executeOperation(SimpleRead1 sr1, GradoopFinbenchBaseGraphState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(sr1.toString());
        DataSet<Tuple3<Date, Boolean, String>> simpleRead1Result = new SimpleRead1GradoopOperator(sr1).execute(connectionState.getGraph());
        List<SimpleRead1Result> simpleRead1Results = new ArrayList<>();
        try {
            simpleRead1Result.collect().forEach(
                tuple -> simpleRead1Results.add(new SimpleRead1Result(tuple.f0, tuple.f1, tuple.f2)));
        } catch (Exception e) {
            throw new DbException("Error while collecting results for simple read 1: " + e);
        }
        resultReporter.report(simpleRead1Results.size(), simpleRead1Results, sr1);
    }
}

class SimpleRead1GradoopOperator implements UnaryBaseGraphToValueOperator<TemporalGraph, DataSet<Tuple3<Date, Boolean, String>>> {
    private final long id;
    public SimpleRead1GradoopOperator(SimpleRead1 simpleRead1) {
        this.id = simpleRead1.getId();
    }

    @Override
    public DataSet<Tuple3<Date, Boolean, String>> execute(TemporalGraph temporalGraph) {
        DataSet<GraphTransaction> accounts = temporalGraph.query("MATCH (a:Account) WHERE a.id = " + id + "L")
            .toGraphCollection()
            .getGraphTransactions();

        List<GraphTransaction> result = new ArrayList<>();
        try {
            result = accounts.collect();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return accounts
            .map(new MapFunction<GraphTransaction, Tuple3<Date, Boolean, String>>() {
                @Override
                public Tuple3<Date, Boolean, String> map(GraphTransaction graphTransaction) throws Exception {
                    Map<String, GradoopId> m = CommonUtils.getVariableMapping(graphTransaction);

                    GradoopId accountId = m.get("a");

                    EPGMVertex account = graphTransaction.getVertexById(accountId);

                    Date createTime = CommonUtils.parseUnixTimeString(account.getPropertyValue("createTime").toString());
                    Boolean isBlocked = account.getPropertyValue("isBlocked").getBoolean();
                    String type = account.getPropertyValue("type").getString();

                    return new Tuple3<>(createTime, isBlocked, type);
                }
            });

    }
}