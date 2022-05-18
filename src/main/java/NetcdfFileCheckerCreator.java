import ucar.nc2.NetcdfFile;

import java.util.concurrent.Callable;

public class NetcdfFileCheckerCreator {

    private NetcdfFileCheckerCreator() {
    }

    public static Callable<Boolean> create(String netcdfFileName, InfoLogger infoLogger) {
        return new NetcdfFileChecker(netcdfFileName, infoLogger);
    }

}
