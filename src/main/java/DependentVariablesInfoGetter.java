import ucar.nc2.NetcdfFile;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class DependentVariablesInfoGetter implements Callable<List<ColumnInfo>> {

    private NetcdfFile netcdfFile;

    public DependentVariablesInfoGetter(NetcdfFile netcdfFile) {
        this.netcdfFile = netcdfFile;
    }


    //TODO: da fare il get fill value
    @Override
    public List<ColumnInfo> call() throws Exception {
        return netcdfFile.getVariables().stream().filter(NetcdfUtilityFunctions::isDependent)
                .map(v -> new ColumnInfo(v.getFullName(), NetcdfUtilityFunctions.getFillValue(v)))
                .collect(Collectors.toList());
    }


}
