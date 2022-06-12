import au.com.bytecode.opencsv.CSVReader;

import java.io.*;
import java.util.*;

public class WritingOnOutputFileTester {

    private static String testFilesFolder = "D:\\UNIVERSITA'\\TESI\\folderForTestingTheSystem";


    private static final String configFileNameForNetcdfPaths = "inputFilesListsForTest.txt";
    private static final String configFileNameFor4DVars = "4DVarsNames.txt";
    private static final String outputFileName = "outputFileForTest.csv";
    private static final String headerFileName = "headerFileForTest.csv";
    private static final String logFileName = "logFile.txt";

    private static final String netcdfTestFilesPrefix = "testFile";
    private static final int nOfNetcdfTestFiles = 5;
    private static final String[] netcdfTestFilesNames = inizializeNetcdfFilesNames();
    private static int[][][][] countMap;


    public static void main(String[] args) {

        if (args.length == 0)
            throw new IllegalArgumentException("Put any folder's path for the test files, as the first argument (no last separator needed)");

        countMap = new int[nOfNetcdfTestFiles][NetcdfTestFilesCreator.DEPTH_DIM_LENGTH][NetcdfTestFilesCreator.LAT_DIM_LENGTH][NetcdfTestFilesCreator.LON_DIM_LENGTH];
        testFilesFolder = args[0];

        boolean runningOnWindows = System.getProperty("os.name").toLowerCase().contains("win");

        if (!runningOnWindows)
            System.out.println("Testing on a not windows machine may not be 100% reliable, anyway it will take some seconds");

        File outputFile = new File(testFilesFolder + File.separator + outputFileName);

        if (outputFile.exists() && !outputFile.delete())
            throw new RuntimeException("Couldn't clear the old output file, test won't proceed");

        writeListInputFiles();
        write4DVarsNamesOnFile();
        writeNetcdfFiles();

        Main.main(new String[]{testFilesFolder + File.separator + configFileNameForNetcdfPaths,
                testFilesFolder + File.separator + outputFileName,
                testFilesFolder + File.separator + headerFileName,
                testFilesFolder + File.separator + configFileNameFor4DVars,
                testFilesFolder + File.separator + logFileName});

        if (runningOnWindows)
            waitUntilOutputFileIsClosed();
        else
            waitForAnEstimatedTime();

        if (isOutputFileCorrect())
            System.out.println("System is working correctly");
        else
            System.out.println("There might be something wrong");

    }

    private static String[] inizializeNetcdfFilesNames() {
        String[] temp = new String[nOfNetcdfTestFiles];
        for (int i = 0; i < nOfNetcdfTestFiles; i++)
            temp[i] = netcdfTestFilesPrefix + i + ".nc";
        return temp;
    }

    private static void writeListInputFiles() {
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(testFilesFolder + File.separator + configFileNameForNetcdfPaths))) {
            for (String name : netcdfTestFilesNames)
                bufferedWriter.write(String.format("%s%n", testFilesFolder + File.separator + name));
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Unable to write the config file for input files list, test won't proceed");
        }
    }

    private static void write4DVarsNamesOnFile() {
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(testFilesFolder + File.separator + configFileNameFor4DVars))) {
            bufferedWriter.write(String.format("%s%n", NetcdfTestFilesCreator.var1TestName));
            bufferedWriter.write(String.format("%s", NetcdfTestFilesCreator.var2TestName));
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Unable to write the config file for 4D vars names, test won't proceed");
        }
    }

    private static void writeNetcdfFiles() {

        for (int i = 0; i < netcdfTestFilesNames.length; i++)
            try {
                NetcdfTestFilesCreator.create(testFilesFolder + File.separator + netcdfTestFilesNames[i], i);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("Cannot create netcdf test files, test won't proceed\nError msg: " + e.toString());
            }

    }

    private static int getFileNumberFromName(String fileName) {
        return Integer.parseInt(fileName.substring(netcdfTestFilesPrefix.length(), fileName.lastIndexOf(".")));
    }


    private static boolean isOutputFileCorrect() {

        try (CSVReader csvReader = new CSVReader(new FileReader(testFilesFolder + File.separator + outputFileName))) {

            List<String[]> rows = csvReader.readAll();
            Map<String, Integer> headerColumns = getVariablesIndexesFromHeader(rows.remove(0));


            for (String[] row : rows) {

                float latValue = Float.parseFloat(row[headerColumns.get(ClassForCostants.latVarName)]);
                float lonValue = Float.parseFloat(row[headerColumns.get(ClassForCostants.lonVarName)]);
                float depthValue = Float.parseFloat(row[headerColumns.get(ClassForCostants.depthVarName)]);
                int fileNumber = getFileNumberFromName(row[headerColumns.get(ClassForCostants.timeVarName)] + ".nc");
                float var1value = Float.parseFloat(row[headerColumns.get(NetcdfTestFilesCreator.var1TestName)]);
                float var2value = Float.parseFloat(row[headerColumns.get(NetcdfTestFilesCreator.var2TestName)]);
                float generatedVar1Value = NetcdfTestFilesCreator.generateValuesForColumns((int) latValue, (int) lonValue, (int) depthValue, 1, fileNumber);
                float generatedVar2Value = NetcdfTestFilesCreator.generateValuesForColumns((int) latValue, (int) lonValue, (int) depthValue, 2, fileNumber);

                countMap[fileNumber][(int) depthValue][(int) latValue][(int) lonValue]++;

                if (generatedVar1Value != var1value || generatedVar2Value != var2value) {
                    System.out.println("It appears (at least) a row is incorrect");
                    return false;
                }

            }
            return isCountMapCorrect();

        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Error happened while reading the csv file, test won't continue");
            return false;
        }
    }

    private static boolean isCountMapCorrect(){
        for(int[][][] fileData: countMap)
            for(int[][] matrixData: fileData)
                for(int[] rowData: matrixData)
                    for(int cellCount: rowData) {
                        if (cellCount == 0) {
                            System.out.println("There is (at least) some data not found in the file");
                        } else if(cellCount > 1){
                            System.out.println("There is (at least) some data duplicated in the file");
                        }
                        if(cellCount != 1)
                            return false;
                    }

        return true;
    }


    private static Map<String, Integer> getVariablesIndexesFromHeader(String[] csvHeader) {
        Map<String, Integer> map = new HashMap<>();

        List<String> columnNames = Arrays.asList(csvHeader);

        map.put(ClassForCostants.lonVarName, columnNames.indexOf(ClassForCostants.lonVarName));
        map.put(ClassForCostants.latVarName, columnNames.indexOf(ClassForCostants.latVarName));
        map.put(ClassForCostants.depthVarName, columnNames.indexOf(ClassForCostants.depthVarName));
        map.put(ClassForCostants.timeVarName, columnNames.indexOf(ClassForCostants.timeVarName));
        map.put(NetcdfTestFilesCreator.var1TestName, columnNames.indexOf(NetcdfTestFilesCreator.var1TestName));
        map.put(NetcdfTestFilesCreator.var2TestName, columnNames.indexOf(NetcdfTestFilesCreator.var2TestName));

        if (map.containsValue(-1))
            throw new RuntimeException("Header seems not correctly formatted, test won't proceed");

        return map;

    }

    private static void waitUntilOutputFileIsClosed() {
        File file = new File(testFilesFolder + File.separator + outputFileName);
        File sameFileName = new File(testFilesFolder + File.separator + outputFileName);
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
            Thread.sleep(20000);
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e.toString());
        }
    }


}