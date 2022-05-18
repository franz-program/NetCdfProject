import java.util.List;
import java.util.concurrent.Callable;

public class DepVariablesInfoGetterCreator {

    private DepVariablesInfoGetterCreator() {
    }

    public static Callable<List<ColumnInfo>> create(String netcdfFileName, InfoLogger infoLogger) {
        return new DependentVariablesInfoGetter(netcdfFileName, infoLogger);
    }


}
