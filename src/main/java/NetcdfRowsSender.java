import ucar.ma2.ArrayFloat;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.io.File;
import java.io.IOException;

public class NetcdfRowsSender implements Runnable {

    private final NetcdfRowsManager rowManager;
    private final String[] columnsNames;
    private NetcdfFile netcdfFile;
    private final String netcdfFileFullPath;
    private final InfoLogger infoLogger;
    private String timeStringRepresentation;


    private ArrayFloat.D4[] columnsValues;
    private ArrayFloat.D1 latData;
    private ArrayFloat.D1 lonData;
    private ArrayFloat.D1 depthData;

    private int latColumnPosition;
    private int lonColumnPosition;
    private int depthColumnPosition;
    private int timeColumnPosition;

    private final static Object MISSING_VALUE = "null";

    private final static String CORRECTLY_READ_MSG = "The file %s was correctly read";
    private final static LogLevel CORRECTLY_READ_LOG_LVL = LogLevel.NORMAL;

    private final static String FILE_WILL_BE_IGNORED_MSG = "The file %s will be not processed";
    private final static LogLevel FILE_WILL_BE_IGNORED_LOG_LVL = LogLevel.WARNING;

    //TODO: se avviene un interruzione esterna allora devo chiudere il file

    public NetcdfRowsSender(NetcdfRowsManager rowManager, String[] columnsNames, String netcdfFileFullPath,
                            InfoLogger infoLogger) {

        this.rowManager = rowManager;
        this.columnsNames = columnsNames;
        this.netcdfFileFullPath = netcdfFileFullPath;
        this.infoLogger = infoLogger;

    }

    @Override
    public void run() {

        try {
            netcdfFile = NetcdfUtilityFunctions.openFile(netcdfFileFullPath);
        } catch (IOException e) {
            rowManager.sourceHasFailed(netcdfFileFullPath);
            infoLogger.log(FILE_WILL_BE_IGNORED_MSG, FILE_WILL_BE_IGNORED_LOG_LVL);
            return;
        }

        try {
            initializeVariables();
        } catch (VariableNotFoundException | IOException e) {
            closeFile();
            rowManager.sourceHasFailed(netcdfFileFullPath);
            infoLogger.log(FILE_WILL_BE_IGNORED_MSG, FILE_WILL_BE_IGNORED_LOG_LVL);
            return;
        }

        Object[] row = new Object[columnsNames.length];

        //TODO: evita che ci sia l'ordine statico time-depth-lat-lon
        for (int i = 0; i < depthData.getSize(); i++) {
            float depthValue = depthData.get(i);
            for (int j = 0; j < latData.getSize(); j++) {
                float latValue = latData.get(j);
                for (int k = 0; k < lonData.getSize(); k++) {

                    row[timeColumnPosition] = timeStringRepresentation;
                    row[depthColumnPosition] = depthValue;
                    row[latColumnPosition] = latValue;
                    row[lonColumnPosition] = lonData.get(k);

                    for (int h = 0; h < columnsValues.length; h++)
                        row[h + 4] = columnsValues[h] == null ? MISSING_VALUE : columnsValues[h].get(0, i, j, k);


                    /*
                     * TODO: il +4 parte dal presupposto che le variabili indipendenti siano nelle prime 4
                     *  posizioni. Va bene supporlo in questa classe oppure no?
                     *  * */
                    rowManager.addRow(row);
                }
            }
        }

        //TODO: catch IndexOutOfBoundException?

        rowManager.sourceHasFinished(netcdfFile.getLocation());
        infoLogger.log(String.format(CORRECTLY_READ_MSG, netcdfFileFullPath), CORRECTLY_READ_LOG_LVL);
        closeFile();

        return;
    }


    private void closeFile() {
        try {
            NetcdfUtilityFunctions.closeFile(netcdfFile);
        } catch (IOException e) {
            //TODO: cosa faccio?
        }
    }

    private void initializeVariables() throws IOException {
        processTimeStringRepresentation();
        processIndependentVarPosition();
        uncatchedInitializeVariables();
    }

    private void processTimeStringRepresentation() {
        String fileName = new File(netcdfFileFullPath).getName();

        try {
            timeStringRepresentation = fileName.substring(fileName.indexOf(".") + 1, fileName.lastIndexOf("."));
        } catch (IndexOutOfBoundsException e) {
            timeStringRepresentation = fileName.substring(0, fileName.lastIndexOf("."));
        }

    }

    private void processIndependentVarPosition() {
        latColumnPosition = findElementIndex(ClassForCostants.latVarName, columnsNames);
        lonColumnPosition = findElementIndex(ClassForCostants.lonVarName, columnsNames);
        depthColumnPosition = findElementIndex(ClassForCostants.depthVarName, columnsNames);
        timeColumnPosition = findElementIndex(ClassForCostants.timeVarName, columnsNames);
        if (latColumnPosition == -1 || lonColumnPosition == -1 || depthColumnPosition == -1 || timeColumnPosition == -1)
            throw new VariableNotFoundException("The fundamental variables names weren't sent to NecdfRowsSender class");

        return;
    }

    private int findElementIndex(String element, String[] array) {

        for (int i = 0; i < array.length; i++)
            if (array[i].equals(element))
                return i;

        return -1;
    }

    private void uncatchedInitializeVariables() throws IOException {

        try {
            depthData = (ArrayFloat.D1) netcdfFile.findVariable(ClassForCostants.depthVarName).read();
            latData = (ArrayFloat.D1) netcdfFile.findVariable(ClassForCostants.latVarName).read();
            lonData = (ArrayFloat.D1) netcdfFile.findVariable(ClassForCostants.lonVarName).read();
        } catch (NullPointerException e) {
            throw new VariableNotFoundException("At least one of the fundamental variables wasn't found in the file " + netcdfFileFullPath);
        }

        //TODO: ocio -4, ocio i = 4
        columnsValues = new ArrayFloat.D4[columnsNames.length - 4];
        for (int i = 4; i < columnsNames.length; i++) {
            Variable var = netcdfFile.findVariable(columnsNames[i]);
            columnsValues[i - 4] = var == null ? null : (ArrayFloat.D4) var.read();
        }

    }
}
