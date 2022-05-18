import au.com.bytecode.opencsv.CSVReader;

import java.io.*;
import java.util.*;

public class WholeSystemTester {

    private static String testFilesFolder = "D:\\UNIVERSITA'\\TESI\\folder for testing the system";
    private static String configFileName = "configFileForTest.txt";
    private static String outputFileName = "outputFileForTest.csv";
    private static String headerFileName = "headerFileForTest.csv";

    private static String netcdfTestFilesPrefix = "testFile";
    private static int nOfNetcdfTestFiles = 5;
    private static String[] netcdfTestFilesNames = inizializeNetcdfFilesNames();


    public static void main(String[] args) {

        if (args.length <= 0)
            throw new IllegalArgumentException("Put any folder's path for the test files, as the first argument (no last separator needed)");

        testFilesFolder = args[0];

        boolean runningOnWindows = System.getProperty("os.name").toLowerCase().contains("win");

        if (!runningOnWindows)
            System.out.println("Testing on a not windows machine may not be 100% reliable, anyway it will take some seconds");

        File outputFile = new File(testFilesFolder + File.separator + outputFileName);

        if (outputFile.exists() && !outputFile.delete())
            throw new RuntimeException("Couldn't clear the old output file, test won't proceed");

        writeOnConfigFile();
        writeNetcdfFiles();

        Main.main(new String[]{testFilesFolder + File.separator + configFileName,
                testFilesFolder + File.separator + outputFileName,
                testFilesFolder + File.separator + headerFileName});

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

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

    private static void writeOnConfigFile() {
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(testFilesFolder + File.separator + configFileName))) {
            for (String name : netcdfTestFilesNames)
                bufferedWriter.write(String.format("%s%n", testFilesFolder + File.separator + name));
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Unable to write the config file, test won't proceed");
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

                if (generatedVar1Value != var1value || generatedVar2Value != var2value)
                    return false;

            }
            return true;

        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Error happened while reading the csv file, test won't continue");
            return false;
        }
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
