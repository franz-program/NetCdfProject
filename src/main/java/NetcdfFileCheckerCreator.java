import ucar.nc2.NetcdfFile;

import java.util.concurrent.Callable;

public class NetcdfFileCheckerCreator {

    private NetcdfFileCheckerCreator(){
    }

    public static Callable<Boolean> createFileChecker(NetcdfFile netcdfFile, InfoLogger infoLogger){
        return new NetcdfFileChecker(netcdfFile, infoLogger);
    }

}
