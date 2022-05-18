import au.com.bytecode.opencsv.CSVReader;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CsvWriterTester {

    private static int nOfRunnables = 10;
    private static int nOfRowsPerRunnable = 100;
    private static int[][] countMap = new int[nOfRunnables][nOfRowsPerRunnable];

    private static String[] columnsNames = new String[]{"runnableNumber", "runnableElement", "generatedValue"};
    private static String csvFileFullPath = "D:\\UNIVERSITA'\\TESI\\folder for testing the system\\csvWriterTestFile.csv";


    public static void main(String[] args) {

        File outputFile = new File(csvFileFullPath);

        if (outputFile.exists() && !outputFile.delete())
            throw new RuntimeException("Couldn't clear the old output file, test won't proceed");

        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() - 1);


        Thread temporaryPostWritingPhaseManager = new ResourceCloser(new UselessInfoLogger(), executorService);
        temporaryPostWritingPhaseManager.start();
        PostWritingPhaseManager postWritingPhaseManager = (PostWritingPhaseManager) temporaryPostWritingPhaseManager;


        NetcdfRowsManager netcdfRowsManager = new CsvWriterFromMultipleFiles(new UselessInfoLogger(), csvFileFullPath, postWritingPhaseManager);

        setRunnablesNames(netcdfRowsManager);
        netcdfRowsManager.setColumnNames(columnsNames);

        startCsvWriter(netcdfRowsManager);

        createRunnables(executorService, netcdfRowsManager);

        boolean runningOnWindows = System.getProperty("os.name").toLowerCase().contains("win");

        if (runningOnWindows)
            waitUntilOutputFileIsClosed();
        else
            waitForAnEstimatedTime();

        if(isOutputFileCorrect())
            System.out.println("Class seems working fine");
        else
            System.out.println("Class seems NOT working correctly");

    }

    private static void startCsvWriter(NetcdfRowsManager netcdfRowsManager){
        Thread thread = (Thread) netcdfRowsManager;
        thread.start();
    }

    private static boolean isOutputFileCorrect() {

        boolean singleRowsAreCorrect = true;

        try (CSVReader csvReader = new CSVReader(new FileReader(csvFileFullPath))) {

            List<String[]> rows = csvReader.readAll();

            rows.remove(0);

            for (String[] row : rows) {

                int runnableNumber = Integer.parseInt(row[0]);
                int runnableRowIndex = Integer.parseInt(row[1]);
                float readGeneratedValue = Float.parseFloat(row[2]);

                countMap[runnableNumber][runnableRowIndex]++;

                if (Arrays.hashCode(new int[]{runnableNumber, runnableRowIndex}) != readGeneratedValue)
                    singleRowsAreCorrect = false;


            }

            if (!singleRowsAreCorrect)
                System.out.println("There is (at least) a row which shouldn't exist");

            return singleRowsAreCorrect && checkIfCountMapCorrect();

        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Error happened while reading the csv file, test won't continue");
            return false;
        }
    }

    private static boolean checkIfCountMapCorrect() {

        boolean emptyCell = false;
        boolean overflowedCell = false;


        for (int[] row : countMap)
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

    private static void setRunnablesNames(NetcdfRowsManager netcdfRowsManager) {
        List<String> runnablesNames = new ArrayList<>();
        for (int i = 0; i < nOfRunnables; i++)
            runnablesNames.add(String.valueOf(i));
        netcdfRowsManager.setSourceNames(runnablesNames);
    }

    private static void createRunnables(ExecutorService executorService, NetcdfRowsManager netcdfRowsManager) {
        for (int i = 0; i < nOfRunnables; i++)
            executorService.execute(new RowsGeneratorForTest(netcdfRowsManager, i, nOfRowsPerRunnable));
    }

    private static void waitUntilOutputFileIsClosed() {
        File file = new File(csvFileFullPath);
        File sameFileName = new File(csvFileFullPath);
        while (true) {
            if (file.renameTo(sameFileName))
                break;
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
                throw new RuntimeException(e.toString());
            }
        }
    }

    private static void waitForAnEstimatedTime() {
        try {
            Thread.sleep(100000);
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e.toString());
        }
    }

}