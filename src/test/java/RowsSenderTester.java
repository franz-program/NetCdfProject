import ucar.ma2.ArrayFloat;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RowsSenderTester implements NetcdfRowsManager {

    private String fileFullPath;

    private String[] columnNames;
    private final int[][][] rowsCountmap;
    private List<Object[]> receivedRows = new ArrayList<>();

    private int depthIndexInRow;
    private int latIndexInRow;
    private int lonIndexInRow;
    private int var1IndexInRow;
    private int var2IndexInRow;

    private boolean failedReading = false;
    private boolean testPassed = true;

    public RowsSenderTester(String fileFullPath) {
        this.fileFullPath = fileFullPath;
        rowsCountmap = new int[NetcdfTestFilesCreator.DEPTH_DIM_LENGTH][NetcdfTestFilesCreator.LAT_DIM_LENGTH][NetcdfTestFilesCreator.LON_DIM_LENGTH];
    }

    @Override
    public void addRow(Object... row) {
        receivedRows.add(row);
        incrementCountmap(row);
    }

    private void modifyCountmap(Object[] row, int step) {
        int depthValue = (int) ((float) row[depthIndexInRow]);
        int latValue = (int) ((float) row[latIndexInRow]);
        int lonValue = (int) ((float) row[lonIndexInRow]);

        rowsCountmap[depthValue][latValue][lonValue] += step;

        if (step == 1)//DEBUG
            System.out.printf("Received %d %d %d\n", depthValue, latValue, lonValue);

        return;
    }

    private void incrementCountmap(Object[] row) {
        modifyCountmap(row, 1);
    }

    private void decrementCountmap(Object[] row) {
        modifyCountmap(row, -1);
    }

    @Override
    public void sourceHasFinished(String sourceName) {
        synchronized (this) {
            failedReading = false;
            this.notify();
        }
    }

    @Override
    public void sourceHasFailed(String sourceName) {
        synchronized (this) {
            failedReading = true;
            this.notify();
        }
    }

    @Override
    public void setColumnNames(String[] columnsNames) {
        this.columnNames = columnsNames;
        initializeRowIndexes();
    }

    private void initializeRowIndexes() {
        List<String> listColumnNames = Arrays.asList(columnNames);
        depthIndexInRow = listColumnNames.indexOf(ClassForCostants.depthVarName);
        latIndexInRow = listColumnNames.indexOf(ClassForCostants.latVarName);
        lonIndexInRow = listColumnNames.indexOf(ClassForCostants.lonVarName);
        var1IndexInRow = listColumnNames.indexOf(NetcdfTestFilesCreator.var1TestName);
        var2IndexInRow = listColumnNames.indexOf(NetcdfTestFilesCreator.var2TestName);
    }

    @Override
    public void run() {
        waitUntilFinishedReading();

        if (failedReading) {
            System.out.println("Something failed while reading the file, test won't proceed");
            return;
        }

        if (!preCheckCountMap())
            testPassed = false;

        try {
            readRowsFromFileAndCompare();
        } catch (IOException e) {
            System.out.println("Couldn't work with file: " + e.toString() + " test won't go on");
            return;
        }

        postCheckCountmap();

        if (testPassed)
            System.out.println("Class seems working fine");
        else
            System.out.println("Class seems not working correctly");


    }

    private void waitUntilFinishedReading() {
        synchronized (this) {
            try {
                this.wait();
            } catch (InterruptedException e) {
            }
        }
    }

    private boolean preCheckCountMap() {

        boolean emptyCell = false;
        boolean overflowedCell = false;

        for (int[][] matrix : rowsCountmap)
            for (int[] row : matrix)
                for (int count : row) {
                    if (count == 0)
                        emptyCell = true;
                    else if (count > 1)
                        overflowedCell = true;
                }

        if (emptyCell)
            System.out.println("There is some element not received");

        if (overflowedCell)
            System.out.println("There is some element received multiple times");

        return !emptyCell && !overflowedCell;

    }

    private void postCheckCountmap() {

        boolean emptyCell = false;

        for (int[][] matrix : rowsCountmap)
            for (int[] row : matrix)
                for (int count : row)
                    if (count != 0)
                        emptyCell = true;

        if (emptyCell && testPassed) {
            System.out.println("The received list is INCONSISTENT");
        }
        testPassed = false;

    }

    private void readRowsFromFileAndCompare() throws IOException {

        NetcdfFile netcdfFile = NetcdfUtilityFunctions.openFile(fileFullPath);

        boolean wrongRowFound = false;

        ArrayFloat.D4 var1Data = getDataOf4DVariable(netcdfFile.findVariable(NetcdfTestFilesCreator.var1TestName));
        ArrayFloat.D4 var2Data = getDataOf4DVariable(netcdfFile.findVariable(NetcdfTestFilesCreator.var2TestName));

        System.out.println("ROWS FROM LIST:");

        for (Object[] row : receivedRows) {
            int depthFileIndex = (int) ((float) row[depthIndexInRow]);
            int latFileIndex = (int) ((float) row[latIndexInRow]);
            int lonFileIndex = (int) ((float) row[lonIndexInRow]);
            if (var1Data.get(0, depthFileIndex, latFileIndex, lonFileIndex) != (float) row[var1IndexInRow]
                    || var2Data.get(0, depthFileIndex, latFileIndex, lonFileIndex) != (float) row[var2IndexInRow]) {
                testPassed = false;
                wrongRowFound = true;
            }
            //DEBUG
            System.out.printf("RIGA: %d, %d, %d %f %f\n", depthFileIndex, latFileIndex, lonFileIndex, (float) row[var1IndexInRow], (float) row[var2IndexInRow]);
            decrementCountmap(row);
        }

        if (wrongRowFound)
            System.out.println("There is (at least) a wrong row received");

        NetcdfUtilityFunctions.closeFile(netcdfFile);

    }

    private ArrayFloat.D4 getDataOf4DVariable(Variable variable) throws IOException {
        try {
            return (ArrayFloat.D4) variable.read(new int[4], variable.getShape());
        } catch (InvalidRangeException e) {
            e.printStackTrace();
            throw new RuntimeException(e.toString());
        }
    }


    @Override
    public void setSourceNames(List<String> sourceNames) {
        return;
    }

}
