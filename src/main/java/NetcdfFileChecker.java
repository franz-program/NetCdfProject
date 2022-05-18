import com.google.common.collect.ImmutableList;
import ucar.ma2.DataType;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

//TODO: guarda altri check che devi fare

/**
 * Given a full path to a netcdf file this class will asynchronously perform some checks on the file:
 * 1) there are only 4D float variables, other than 'depth', 'lat', 'lon'
 * 2) 'depth', 'lat', 'lon' are 1D and have these names
 * 3) each 4D variable must have the same dimension size to the size of the correspondent 1D variable
 * 4) time dimension's size must be one
 * 5) The 4D variables should depend on 1D variables in a coherent way, e.g. if time is the first dimension then it should be for every 4D variable
 */
public class NetcdfFileChecker implements Callable<Boolean> {

    private final static String[] CORRECT_DIMENSIONS_NAMES = {ClassForCostants.lonVarName,
            ClassForCostants.latVarName, ClassForCostants.depthVarName, ClassForCostants.timeVarName};
    private final String[] CORRECT_DIMENSIONS_NAMES_WITH_SIZE_ONE = {ClassForCostants.timeVarName};

    private NetcdfFile netcdfFile;
    private final String netcdfFileFullPath;
    private InfoLogger infoLogger;

    private final String INCORRECT_FILE_STRUCTURE_MSG = "The file %s seems not to follow the correct internal" +
            "structure";
    private final LogLevel INCORRECT_FILE_STRUCTURE_LOG_LVL = LogLevel.WARNING;

    private final static String TEST_PASSED_MSG = "The file %s has passed preprocessing";
    private final static LogLevel TEST_PASSED_LOG_LVL = LogLevel.NORMAL;

    private final static String UNABLE_TO_OPEN_FILE_MSG = "The file %s failed to open";
    private final static LogLevel UNABLE_TO_OPEN_FILE_LOG_LVL = LogLevel.WARNING;

    public NetcdfFileChecker(String netcdfFileFullPath, InfoLogger infoLogger) {
        this.netcdfFileFullPath = netcdfFileFullPath;

        //TODO: check che sia un file netcdf

        this.infoLogger = infoLogger;
    }

    @Override
    public Boolean call() throws IOException {


        try {
            netcdfFile = NetcdfUtilityFunctions.openFile(netcdfFileFullPath);
        } catch (IOException e) {
            infoLogger.log(UNABLE_TO_OPEN_FILE_MSG, UNABLE_TO_OPEN_FILE_LOG_LVL);
            throw e;
        }

        boolean testPassed = processTest();

        if (testPassed)
            infoLogger.log(String.format(TEST_PASSED_MSG, netcdfFileFullPath), TEST_PASSED_LOG_LVL);
        else
            infoLogger.log(INCORRECT_FILE_STRUCTURE_MSG,
                    INCORRECT_FILE_STRUCTURE_LOG_LVL);

        try {
            NetcdfUtilityFunctions.closeFile(netcdfFile);
        } catch (IOException e) {
            //TODO: log
            throw e;
        }

        return testPassed;

    }

    private boolean processTest() {
        return check1DVariables()
                && checkDimensionAndTypeVariables()
                && checkCoherentSizeAndOrder4DVariables();
    }

    private boolean check1DVariables() {
        List<String> indepVar = netcdfFile.getVariables().stream()
                .filter(v -> v.getDimensions().size() == 1)
                .map(Variable::getFullName)
                .collect(Collectors.toList());

        return indepVar.size() == 3
                && indepVar.contains(ClassForCostants.depthVarName)
                && indepVar.contains(ClassForCostants.latVarName)
                && indepVar.contains(ClassForCostants.lonVarName);
    }

    private boolean checkDimensionAndTypeVariables() {
        return netcdfFile.getVariables().stream()
                .noneMatch(v -> (v.getDimensions().size() != 4 && v.getDimensions().size() != 1))
                && (netcdfFile.getVariables().stream()
                .filter(v -> v.getDimensions().size() == 4)
                .filter(v -> !v.getDataType().equals(DataType.FLOAT))
                .count() == 0);
    }

    private boolean checkCoherentSizeAndOrder4DVariables() {
        List<Dimension> dimensionsOf1DVar = get1DVarDimensions();
        List<String> var1DNames = dimensionsOf1DVar.stream()
                .map(Dimension::getName)
                .collect(Collectors.toList());


        List<List<String>> distinctDimensionsNames4DVariables = getDistinctDimensionsNames4DVariables();

        if(distinctDimensionsNames4DVariables.size() != 1)
            return false;

        List<String> names4DVariables = distinctDimensionsNames4DVariables.get(0);

        for(String dimName: var1DNames){
            if(!names4DVariables.contains(dimName))
                return false;
        }

        return true;
    }

    private List<List<String>> getDistinctDimensionsNames4DVariables(){
        return netcdfFile.getVariables().stream()
                .filter(v -> v.getDimensions().size() == 4)
                .map(Variable::getDimensions)
                .map(list -> list.stream().map(Dimension::getName).collect(Collectors.toList()))
                .distinct()
                .collect(Collectors.toList());
    }

    private List<Dimension> get1DVarDimensions(){
        List<ImmutableList<Dimension>> indepVar = netcdfFile.getVariables().stream()
                .filter(v -> v.getDimensions().size() == 1)
                .map(Variable::getDimensions)
                .collect(Collectors.toList());

        List<Dimension> list = new ArrayList<>();

        for(ImmutableList<Dimension> immutableList : indepVar)
            list.addAll(immutableList);

        return list;
    }

}
