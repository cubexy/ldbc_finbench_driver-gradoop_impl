package org.ldbcouncil.finbench.impls.gradoop.queries;

import org.ldbcouncil.finbench.driver.util.Tuple;

public interface ArgArrayToInputDataTuple {
    Tuple getInputDataTuple(Object[] args);
}
