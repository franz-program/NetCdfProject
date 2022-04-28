import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.util.concurrent.Callable;


//TODO: rifai in modo un po' più pulito
//TODO: guarda altri check che devi fare

public class NetcdfFileChecker implements Callable<Boolean> {

    //TODO: static da problemi con multithreading?
    private final static String[] CORRECT_DIMENSIONS_NAMES = {ClassForCostants.lonVarName,
            ClassForCostants.latVarName, ClassForCostants.depthVarName, ClassForCostants.timeVarName};
    private final static String DIMENSION_NAME_WITH_ONLY_ONE_VALUE = CORRECT_DIMENSIONS_NAMES[3];

    private NetcdfFile netcdfFile;
    private InfoLogger infoLogger;

    private final static String INCORRECT_FILE_STRUCTURE_MSG = "The file %s seems not to follow the correct internal" +
            "structure%n";
    private final static LogLevel INCORRECT_FILE_STRUCTURE_LOG_LVL = LogLevel.ERROR;
    private final static String CORRECT_STRUCTURE_RULES_MSG = "The rules are:%n1. variables with more than one dimension" +
            "can depend only on " + getStringRapresentationOfCorrectDimNames() +
            "%n2. if a variable with more than one dimension depend on " + DIMENSION_NAME_WITH_ONLY_ONE_VALUE +
            "then the size of this dimension must be 1";

    public NetcdfFileChecker(NetcdfFile netcdfFile, InfoLogger infoLogger) {
        this.netcdfFile = netcdfFile;
        this.infoLogger = infoLogger;
    }

    @Override
    public Boolean call() throws Exception {
        boolean bool = netcdfFile.getVariables().stream().
                noneMatch(v -> NetcdfUtilityFunctions.isDependent(v) && hasIncorrectDimensions(v));

        if (!bool) {
            infoLogger.log(INCORRECT_FILE_STRUCTURE_MSG + CORRECT_STRUCTURE_RULES_MSG
                    , INCORRECT_FILE_STRUCTURE_LOG_LVL);
        }

        return bool;

    }



    private boolean hasIncorrectDimensions(Variable variable) {

        for (Dimension d : variable.getDimensions()) {

            boolean incorrectDim = true;
            for (String dimName : CORRECT_DIMENSIONS_NAMES) {
                if (d.getName().equals(dimName))
                    incorrectDim = false;
                if (d.getName().equals(DIMENSION_NAME_WITH_ONLY_ONE_VALUE) && d.getLength() != 1)
                    return true;
            }

            if (incorrectDim)
                return true;
        }
        return false;
    }

    private static String getStringRapresentationOfCorrectDimNames() {
        String rapresentation = "";
        for (String name : CORRECT_DIMENSIONS_NAMES)
            rapresentation += name + ", ";
        rapresentation = rapresentation.substring(0, rapresentation.length() - 2);
        return rapresentation;
    }


}
