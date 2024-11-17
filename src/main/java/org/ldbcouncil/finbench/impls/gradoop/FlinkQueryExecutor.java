package org.ldbcouncil.finbench.impls.gradoop;
import static org.ldbcouncil.finbench.impls.gradoop.CommonUtils.parseUnixTimeString;

import java.util.Date;

public class FlinkQueryExecutor {

    public static void main(String[] args) {
        FlinkCmdArgParser parser = new FlinkCmdArgParser(args);
        parser.init();
        parser.parse();

    }
}
