package org.ldbcouncil.finbench.impls.gradoop;

import org.ldbcouncil.finbench.driver.DbConnectionState;

import java.io.IOException;

public class DummyDbConnectionState extends DbConnectionState {

    public DummyDbConnectionState() {
    }

    @Override
    public void close() throws IOException {
    }

}
