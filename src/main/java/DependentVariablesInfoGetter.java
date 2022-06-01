import ucar.nc2.Variable;
import java.util.List;
import java.util.stream.Collectors;

public class DependentVariablesInfoGetter {

    //TODO: da fare il get fill value

    public static List<ColumnInfo> processColumnInfo(List<Variable> dependentVariables) {
        return dependentVariables.stream()
                .map(v -> new ColumnInfo(v.getFullName(), NetcdfUtilityFunctions.getFillValue(v)))
                .collect(Collectors.toList());
    }


}
