import ucar.ma2.ArrayFloat;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

public class NetcdfDataReaderTester implements AsynchronousOutputFileWriter, AsynchronousFilesValidityCollector, AsynchronousFailedFilesManager, HeaderFileWriter {

    private String fileFullPath;

    private String[] columnNames;
    private final int[][][] rowsCountmap;
    private DataGetter receivedData;

    private int depthIndexInRow;
    private int latIndexInRow;
    private int lonIndexInRow;
    private int var1IndexInRow;
    private int var2IndexInRow;

    private boolean failedReading = false;
    private boolean testPassed = true;

    public NetcdfDataReaderTester(String fileFullPath, String[] columnNames) {
        this.fileFullPath = fileFullPath;
        this.columnNames = columnNames;
        rowsCountmap = new int[NetcdfTestFilesCreator.DEPTH_DIM_LENGTH][NetcdfTestFilesCreator.LAT_DIM_LENGTH][NetcdfTestFilesCreator.LON_DIM_LENGTH];
        initializeRowIndexes();
    }

    @Override
    public void addDataGetter(DataGetter dataGetter) {
        receivedData = dataGetter;
    }

    private void modifyCountmap(Object[] row, int step) {
        int depthValue = (int) ((float) row[depthIndexInRow]);
        int latValue = (int) ((float) row[latIndexInRow]);
        int lonValue = (int) ((float) row[lonIndexInRow]);

        rowsCountmap[depthValue][latValue][lonValue] += step;

        return;
    }

    private void incrementCountmap(Object[] row) {
        modifyCountmap(row, 1);
    }

    @Override
    public void sourceHasFinished(String sourceName) {
        synchronized (this) {
            failedReading = false;
            this.notify();
        }
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

        try {
            readRowsFromFileAndCompare();
        } catch (IOException e) {
            System.out.println("Couldn't work with file: " + e.toString() + " test won't go on");
            return;
        }

        checkCountmap();

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

    private void checkCountmap() {

        for (int[][] matrix : rowsCountmap)
            for (int[] row : matrix)
                for (int count : row)
                    if (count != 1) {
                        testPassed = false;
                        return;
                    }
        return;
    }

    private void readRowsFromFileAndCompare() throws IOException {

        NetcdfFile netcdfFile = NetcdfUtilityFunctions.openFile(fileFullPath);

        boolean wrongRowFound = false;

        ArrayFloat.D4 var1Data = getDataOf4DVariable(netcdfFile.findVariable(NetcdfTestFilesCreator.var1TestName));
        ArrayFloat.D4 var2Data = getDataOf4DVariable(netcdfFile.findVariable(NetcdfTestFilesCreator.var2TestName));

        receivedData.readFromBeginning();

        System.out.println("ROWS FROM LIST:");

        while (true) {

            Object[] row;
            try {
                row = receivedData.getNextRow();
            } catch (NoSuchElementException e) {
                break;
            }

            int depthFileIndex = (int) ((float) row[depthIndexInRow]);
            int latFileIndex = (int) ((float) row[latIndexInRow]);
            int lonFileIndex = (int) ((float) row[lonIndexInRow]);
            if (var1Data.get(0, depthFileIndex, latFileIndex, lonFileIndex) != (float) row[var1IndexInRow]
                    || var2Data.get(0, depthFileIndex, latFileIndex, lonFileIndex) != (float) row[var2IndexInRow]) {
                testPassed = false;
                wrongRowFound = true;
            }
            incrementCountmap(row);
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
    public void setFailedFile(String filePath) {

    }

    @Override
    public void setFileValidity(String filename, boolean isValid) {

    }

    @Override
    public void addColumnsInfo(List<ColumnInfo> columnsInfo) {

    }

    @Override
    public void startWriting() {

    }
}
