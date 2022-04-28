import ucar.nc2.NetcdfFile;

import java.util.List;
import java.util.concurrent.Callable;

public class DepVariablesInfoGetterCreator {

    private DepVariablesInfoGetterCreator(){
    }

    public static Callable<List<ColumnInfo>> createVariablesInfoGetter(NetcdfFile netcdfFile){
        return new DependentVariablesInfoGetter(netcdfFile);
    }


}
