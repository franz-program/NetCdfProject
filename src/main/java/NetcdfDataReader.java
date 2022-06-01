import ucar.ma2.ArrayFloat;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NetcdfDataReader implements Runnable {

    private final AsynchronousOutputFileWriter outputFileWriter;
    private final InfoLogger infoLogger;
    private final AsynchronousFilesValidityCollector filesValidityCollector;
    private final AsynchronousFailedFilesManager failedFilesManager;
    private final HeaderFileWriter headerFileWriter;

    private final String[] columnsNames;
    private NetcdfFile netcdfFile;
    private final String netcdfFileFullPath;

    private ArrayFloat.D4[] columnsValues;
    private ArrayFloat.D1 latData;
    private ArrayFloat.D1 lonData;
    private ArrayFloat.D1 depthData;

    private final List<Variable> depVariables;

    private final int depthColumnPosition = 0;
    private final int latColumnPosition = 1;
    private final int lonColumnPosition = 2;

    private final static float MISSING_VALUE = Float.MIN_VALUE;

    private final static String CORRECTLY_READ_MSG = "The file %s was correctly read";
    private final static LogLevel CORRECTLY_READ_LOG_LVL = LogLevel.NORMAL;

    private final static String CANNOT_CLOSE_FILE_MSG = "Couldn't close file %s";
    private final static LogLevel CANNOT_CLOSE_FILE_LOG_LVL = LogLevel.ERROR;

    private final static String CANNOT_OPEN_FILE_MSG = "Couldn't open file %s";
    private final static LogLevel CANNOT_OPEN_FILE_LOG_LVL = LogLevel.ERROR;

    private final static String CANNOT_INITIALISE_VAR_MSG = "Couldn't initialise variables of file %s";
    private final static LogLevel CANNOT_INITIALISE_VAR_LOG_LVL = LogLevel.ERROR;

    private final static String GENERAL_EXCEPTION_MSG = "Exception happened";
    private final static LogLevel GENERAL_EXCEPTION_LOG_LVL = LogLevel.ERROR;

    private final static String ADD_EXCEPTION_MSG = "because %s happened";

    private final static String VAR_NOT_FOUND_MSG = "The variable %s wasn't found in %s, it will be print %f as " +
            "a filling value for this file's %s's data";
    private final static LogLevel VAR_NOT_FOUND_LOG_LVL = LogLevel.WARNING;


    //TODO: se avviene un interruzione esterna allora devo chiudere il file

    public NetcdfDataReader(AsynchronousOutputFileWriter outputFileWriter, String[] columnsNames, String netcdfFileFullPath,
                            InfoLogger infoLogger, AsynchronousFilesValidityCollector filesValidityCollector,
                            AsynchronousFailedFilesManager failedFilesManager, HeaderFileWriter headerFileWriter) {

        this.outputFileWriter = outputFileWriter;
        this.columnsNames = columnsNames;
        this.netcdfFileFullPath = netcdfFileFullPath;
        this.infoLogger = infoLogger;
        this.filesValidityCollector = filesValidityCollector;
        this.failedFilesManager = failedFilesManager;
        this.headerFileWriter = headerFileWriter;
        depVariables = new ArrayList<>();
        processIndependentVarPosition();
    }

    private void processIndependentVarPosition() {
        int latColumnPosition = findElementIndex(ClassForCostants.latVarName, columnsNames);
        int lonColumnPosition = findElementIndex(ClassForCostants.lonVarName, columnsNames);
        int depthColumnPosition = findElementIndex(ClassForCostants.depthVarName, columnsNames);
        if (latColumnPosition != this.latColumnPosition ||
                lonColumnPosition != this.lonColumnPosition ||
                depthColumnPosition != this.depthColumnPosition)
            throw new IllegalArgumentException("The fundamental variables names aren't in the correct positions");

        return;
    }

    private int findElementIndex(String element, String[] array) {

        for (int i = 0; i < array.length; i++)
            if (array[i].equals(element))
                return i;

        return -1;
    }

    @Override
    public void run() {

        try {
            netcdfFile = NetcdfUtilityFunctions.openFile(netcdfFileFullPath);
        } catch (IOException e) {
            failedFilesManager.setFailedFile(netcdfFileFullPath);

            String logMsg = String.format(CANNOT_OPEN_FILE_MSG, netcdfFileFullPath);
            String exceptionMsg = String.format(ADD_EXCEPTION_MSG, e.getMessage());
            infoLogger.log(logMsg + " " + exceptionMsg, CANNOT_OPEN_FILE_LOG_LVL);
            return;
        }

        boolean fileValidity = checkFileValidity();
        filesValidityCollector.setFileValidity(netcdfFileFullPath, fileValidity);

        if (!fileValidity) {
            try {
                NetcdfUtilityFunctions.closeFile(netcdfFile);
            } catch (IOException e) {
                infoLogger.log(String.format(CANNOT_CLOSE_FILE_MSG, netcdfFileFullPath)
                        + " " + String.format(ADD_EXCEPTION_MSG, e.getMessage()),
                        CANNOT_CLOSE_FILE_LOG_LVL);
                System.gc();
                return;
            }
        }

        if(!initializeVariablesAndComunicateIfOkay())
            return;

        headerFileWriter.addColumnsInfo(DependentVariablesInfoGetter.processColumnInfo(depVariables));

        DataReceiver dataReceiver = DataReceiverCreator.createDataReceiver((int) (depthData.getSize() * latData.getSize() * lonData.getSize()));
        dataReceiver.setSourceName(netcdfFileFullPath);

        try {
            sendRows(dataReceiver);
        } catch(RuntimeException e){
            failedFilesManager.setFailedFile(netcdfFileFullPath);
            infoLogger.log(GENERAL_EXCEPTION_MSG + " " +
                            String.format(ADD_EXCEPTION_MSG, e.getMessage()),
                    GENERAL_EXCEPTION_LOG_LVL);
        }

        infoLogger.log(String.format(CORRECTLY_READ_MSG, netcdfFileFullPath),
                CORRECTLY_READ_LOG_LVL);

        try {
            NetcdfUtilityFunctions.closeFile(netcdfFile);
        } catch (IOException e) {
            infoLogger.log(String.format(CANNOT_CLOSE_FILE_MSG, netcdfFileFullPath)
                    + " " + String.format(ADD_EXCEPTION_MSG, e.getMessage()),
                    CANNOT_CLOSE_FILE_LOG_LVL);
        }

        outputFileWriter.addDataGetter(dataReceiver.getDataGetter());
        outputFileWriter.sourceHasFinished(netcdfFileFullPath);

        System.gc();

        return;
    }

    private boolean checkFileValidity() {
        try {
            return NetcdfFileCheckerCreator.create(netcdfFile).call();
        } catch (Exception e) {
            return false;
        }
    }

    private void sendRows(DataReceiver dataReceiver){
        for (int i = 0; i < depthData.getSize(); i++) {
            float depthValue = depthData.get(i);
            for (int j = 0; j < latData.getSize(); j++) {
                float latValue = latData.get(j);
                for (int k = 0; k < lonData.getSize(); k++) {

                    float[] row = new float[columnsNames.length];

                    row[depthColumnPosition] = depthValue;
                    row[latColumnPosition] = latValue;
                    row[lonColumnPosition] = lonData.get(k);

                    for (int h = 0; h < columnsValues.length; h++)
                        row[h + 3] = columnsValues[h] == null ? MISSING_VALUE : columnsValues[h].get(0, i, j, k);

                    dataReceiver.sendRow(row);
                }
            }
        }
    }

    private boolean initializeVariablesAndComunicateIfOkay(){
        try {
            initializeVariables();
            return true;
        } catch (FundamentalVariableNotFoundException | IOException e) {
            failedFilesManager.setFailedFile(netcdfFileFullPath);

            if (e instanceof FundamentalVariableNotFoundException) {
                infoLogger.log(String.format(CANNOT_INITIALISE_VAR_MSG, netcdfFileFullPath)
                                + " " + String.format(ADD_EXCEPTION_MSG, e.getMessage()),
                        CANNOT_INITIALISE_VAR_LOG_LVL);
            }

            try {
                NetcdfUtilityFunctions.closeFile(netcdfFile);
            } catch (IOException ex) {
                infoLogger.log(String.format(CANNOT_CLOSE_FILE_MSG, netcdfFileFullPath)
                                + " " + String.format(ADD_EXCEPTION_MSG, ex.getMessage()),
                        CANNOT_CLOSE_FILE_LOG_LVL);
            }
            System.gc();
            return false;
        }
    }

    private void initializeVariables() throws IOException {

        try {
            depthData = (ArrayFloat.D1) netcdfFile.findVariable(ClassForCostants.depthVarName).read();
            latData = (ArrayFloat.D1) netcdfFile.findVariable(ClassForCostants.latVarName).read();
            lonData = (ArrayFloat.D1) netcdfFile.findVariable(ClassForCostants.lonVarName).read();
        } catch (NullPointerException e) {
            throw new FundamentalVariableNotFoundException("At least one of the fundamental variables wasn't found in the file " + netcdfFileFullPath);
        }

        columnsValues = new ArrayFloat.D4[columnsNames.length - 3];
        for (int i = 3; i < columnsNames.length; i++) {
            Variable var = netcdfFile.findVariable(columnsNames[i]);
            columnsValues[i - 3] = var == null ? null : (ArrayFloat.D4) var.read();
            if (var == null)
                infoLogger.log(String.format(VAR_NOT_FOUND_MSG, columnsNames[i - 3], netcdfFileFullPath,
                        MISSING_VALUE, columnsNames[i - 3]), VAR_NOT_FOUND_LOG_LVL);
            else
                depVariables.add(var);
        }

    }


}
