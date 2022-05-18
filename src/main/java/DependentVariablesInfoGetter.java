import ucar.nc2.NetcdfFile;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class DependentVariablesInfoGetter implements Callable<List<ColumnInfo>> {

    private NetcdfFile netcdfFile;
    private final String netcdfFileName;
    private InfoLogger infoLogger;

    private final static String UNABLE_TO_OPEN_FILE_MSG = "The file %s failed to open, it will be ignored";
    private final static LogLevel UNABLE_TO_OPEN_FILE_LOG_LVL = LogLevel.WARNING;

    public DependentVariablesInfoGetter(String netcdfFileName,
                                        InfoLogger infoLogger) {
        this.netcdfFileName = netcdfFileName;
        this.infoLogger = infoLogger;
    }


    //TODO: da fare il get fill value
    @Override
    public List<ColumnInfo> call() throws IOException {

        try {
            netcdfFile = NetcdfUtilityFunctions.openFile(netcdfFileName);
        } catch (IOException e) {
            infoLogger.log(UNABLE_TO_OPEN_FILE_MSG, UNABLE_TO_OPEN_FILE_LOG_LVL);
            throw e;
        }

        List<ColumnInfo> columnInfos = processColumnInfo();

        try {
            NetcdfUtilityFunctions.closeFile(netcdfFile);
            return columnInfos;
        } catch (IOException e) {
            throw e;
        }

    }

    private List<ColumnInfo> processColumnInfo() {
        return netcdfFile.getVariables().stream().filter(v -> v.getDimensions().size() == 4)
                .map(v -> new ColumnInfo(v.getFullName(), NetcdfUtilityFunctions.getFillValue(v)))
                .collect(Collectors.toList());
    }


}
