import ucar.ma2.ArrayFloat;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

public class NetcdfRowsSender implements Runnable {

    private final AsynchronousTableWriter tableWriter;
    private final String[] columnsNames;
    private final NetcdfFile netcdfFile;
    private final InfoLogger infoLogger;
    private String timeStringRepresentation;

    private ArrayFloat.D4[] columnsValues;
    private ArrayFloat.D1 lat;
    private ArrayFloat.D1 lon;
    private ArrayFloat.D1 depth;

    private int latColumnPosition;
    private int lonColumnPosition;
    private int depthColumnPosition;
    private int timeColumnPosition;

    private final static Object MISSING_VALUE = "null";
    
    private final static String CORRECTLY_READ_MSG = "The file %s was correclty read";
    private final static LogLevel CORRECTLY_READ_LOG_LVL = LogLevel.NORMAL;


    public NetcdfRowsSender(AsynchronousTableWriter tableWriter, String[] columnsNames, NetcdfFile netcdfFile,
                            InfoLogger infoLogger) {

        this.tableWriter = tableWriter;
        this.columnsNames = columnsNames;
        this.netcdfFile = netcdfFile;
        this.infoLogger = infoLogger;

        processTimeStringRepresentation();
        processIndependentVarPosition();
    }

    private void processTimeStringRepresentation() {
        String fileName = new File(netcdfFile.getLocation()).getName();

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
    }

    private int findElementIndex(String element, String[] array) {

        for (int i = 0; i < array.length; i++)
            if (array[i].equals(element))
                return i;

        return -1;
    }


    @Override
    public void run() {

        initializeVariables();

        Object[] row = new Object[columnsNames.length];

        //TODO: evita che ci sia l'ordine statico time-depth-lat-lon
        for (int i = 0; i < depth.getSize(); i++) {
            float tempI = depth.get(i);
            for (int j = 0; j < lat.getSize(); j++) {
                float tempJ = lat.get(j);
                for (int k = 0; k < lon.getSize(); k++) {
                    row[timeColumnPosition] = timeStringRepresentation;
                    row[depthColumnPosition] = tempI;
                    row[latColumnPosition] = tempJ;
                    row[lonColumnPosition] = lon.get(k);
                    for (int h = 0; h < columnsValues.length; h++)
                        row[h + 4] = Objects.isNull(columnsValues[h]) ? MISSING_VALUE : columnsValues[h].get(0, i, j, k);

                    /*
                     * TODO: il +4 parte dal presupposto che le variabili indipendenti siano nelle prime 4
                     *  posizioni. Va bene supporlo in questa classe oppure no?
                     *
                     *
                     *  * */

                    tableWriter.writeRow(row);
                }
            }
        }
        //TODO: catch IndexOutOfBoundException?

        tableWriter.sourceHasFinished(netcdfFile.getLocation());

        infoLogger.log(CORRECTLY_READ_MSG, CORRECTLY_READ_LOG_LVL);

        return;
    }

    private void initializeVariables() {
        try {
            uncatchedInitializeVariables();
        } catch (IOException e) {
            //TODO: chiudo tutto?
            //TODO: log
        }
    }

    private void uncatchedInitializeVariables() throws IOException {
        
        //TODO: occhio alle NullException se non trova le variabili
        depth = (ArrayFloat.D1) netcdfFile.findVariable(ClassForCostants.depthVarName).read();
        lat = (ArrayFloat.D1) netcdfFile.findVariable(ClassForCostants.latVarName).read();
        lon = (ArrayFloat.D1) netcdfFile.findVariable(ClassForCostants.lonVarName).read();

        //TODO: ocio -4, ocio i = 4
        columnsValues = new ArrayFloat.D4[columnsNames.length - 4];
        for (int i = 4; i < columnsNames.length; i++) {
            Variable var = netcdfFile.findVariable(columnsNames[i]);
            //TODO: controlla che restituisca null se non trova la variabile
            columnsValues[i] = Objects.isNull(var) ? null : (ArrayFloat.D4) var.read();
        }

    }
}
