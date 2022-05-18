import ucar.ma2.DataType;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TesterNetcdfFileChecker {

    private static String folderNameForTestFiles = "D:\\UNIVERSITA'\\TESI\\cartella";
    private static String correctFileName = "correct.nc";
    private static String permutatedFileName = "permutated.nc";
    private static String incoherentDimensionsFileName = "incoherent.nc";
    private static Map<String, Boolean> mapOfFilesToTest = getMapOfFilesToTest();
    private static List<String> listOfTestFilesNames = new ArrayList<>(mapOfFilesToTest.keySet());
    private static InfoLogger infoLoggerForTest = new UselessInfoLogger();
    private static List<String> failedProcessedFilesNames = new ArrayList<>();
    private static ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());


    public static void main(String[] args) {

        filterInvalidFiles();
        if (checkWhichFilesFailedTheTest() && checkWhichFilesPassedTheTest())
            System.out.println("CLASS WORKS FINE");
        else
            System.out.println("THERE MIGHT BE SOME ERRORS IN THE CLASS");

        executorService.shutdown();
        executorService.shutdownNow();
    }

    private static boolean checkWhichFilesPassedTheTest() {
        return listOfTestFilesNames.size() == 1
                && listOfTestFilesNames.contains(folderNameForTestFiles + File.separator + correctFileName);

    }

    private static boolean checkWhichFilesFailedTheTest() {
        return failedProcessedFilesNames.size() == 2
                && failedProcessedFilesNames.contains(folderNameForTestFiles + File.separator + permutatedFileName)
                && failedProcessedFilesNames.contains(folderNameForTestFiles + File.separator + incoherentDimensionsFileName);
    }

    private static void filterInvalidFiles() {

        Map<String, Future<Boolean>> futureMap = createFutureMapForFirstCheck();

        try {

            for (Map.Entry<String, Future<Boolean>> pair : futureMap.entrySet()) {
                try {
                    processTestedFile(pair.getValue().get(), pair.getKey());
                } catch (ExecutionException e) {
                    e.printStackTrace();
                    throw new RuntimeException("An exception happened while processing the files: " + e.toString());
                }
            }

        } catch (InterruptedException e) {
            infoLoggerForTest.log(e.toString(), LogLevel.ERROR);
            //TODO: quando avviene?
            //TODO: cosa faccio?
        }
    }

    private static Map<String, Future<Boolean>> createFutureMapForFirstCheck() {
        //TODO: qual è la migliore implementazione?
        Map<String, Future<Boolean>> map = new HashMap<>();

        for (String netcdfFileName : listOfTestFilesNames)
            map.put(netcdfFileName, executorService.submit(NetcdfFileCheckerCreator.create(netcdfFileName, infoLoggerForTest)));

        return map;
    }

    private static void processTestedFile(boolean testResult, String fileFullPath) {
        if (!testResult) {
            listOfTestFilesNames.remove(fileFullPath);
            failedProcessedFilesNames.add(fileFullPath);
        }
    }

    private static Map<String, Boolean> getMapOfFilesToTest() {


        try {
            createCorrectNetcdfFile(folderNameForTestFiles + File.separator + correctFileName);
            createNetcdfFileWithDifferentDimSizes(folderNameForTestFiles + File.separator + incoherentDimensionsFileName);
            createNetcdfFileWithPermutatedDimensions(folderNameForTestFiles + File.separator + permutatedFileName);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Test files couldn't be opened, test won't proceed: " + e.toString());

        }

        Map<String, Boolean> map = new HashMap<>();
        map.put(folderNameForTestFiles + File.separator + correctFileName, true);
        map.put(folderNameForTestFiles + File.separator + permutatedFileName, false);
        map.put(folderNameForTestFiles + File.separator + incoherentDimensionsFileName, false);

        return map;
    }

    private static void createNetcdfFileWithPermutatedDimensions(String filePath) throws IOException {

        final int NLAT = 6;
        final int NLON = 12;
        final int NDEPTH = 10;

        NetcdfFileWriter dataFile = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, filePath);

        Dimension lat = dataFile.addDimension(null, ClassForCostants.latVarName, NLAT);
        Dimension lon = dataFile.addDimension(null, ClassForCostants.lonVarName, NLON);
        Dimension depth = dataFile.addDimension(null, ClassForCostants.depthVarName, NDEPTH);
        Dimension time = dataFile.addDimension(null, ClassForCostants.timeVarName, 1);

        List<Dimension> dims = new ArrayList<>();
        dims.add(lat);
        dims.add(lon);
        dims.add(depth);
        dims.add(time);

        List<Dimension> permutatedDims = new ArrayList<>();
        permutatedDims.add(lat);
        permutatedDims.add(lon);
        permutatedDims.add(time);
        permutatedDims.add(depth);

        List<Dimension> listForLon = new ArrayList<>();
        listForLon.add(lon);

        List<Dimension> listForLat = new ArrayList<>();
        listForLat.add(lat);

        List<Dimension> listForDepth = new ArrayList<>();
        listForDepth.add(depth);

        Variable dataVariable = dataFile.addVariable(null, "Var1", DataType.FLOAT, dims);
        Variable dataVariable2 = dataFile.addVariable(null, "Var2", DataType.FLOAT, permutatedDims);
        Variable varLon = dataFile.addVariable(null, ClassForCostants.lonVarName, DataType.FLOAT, listForLon);
        Variable varLat = dataFile.addVariable(null, ClassForCostants.latVarName, DataType.FLOAT, listForLat);
        Variable varDepth = dataFile.addVariable(null, ClassForCostants.depthVarName, DataType.FLOAT, listForDepth);


        dataFile.create();

        return;
    }

    private static void createCorrectNetcdfFile(String filePath) throws IOException {

        final int NLAT = 6;
        final int NLON = 12;
        final int NDEPTH = 10;


        NetcdfFileWriter dataFile = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, filePath);

        Dimension lat = dataFile.addDimension(null, ClassForCostants.latVarName, NLAT);
        Dimension lon = dataFile.addDimension(null, ClassForCostants.lonVarName, NLON);
        Dimension depth = dataFile.addDimension(null, ClassForCostants.depthVarName, NDEPTH);
        Dimension time = dataFile.addDimension(null, ClassForCostants.timeVarName, 1);

        List<Dimension> dims = new ArrayList<>();
        dims.add(lat);
        dims.add(lon);
        dims.add(depth);
        dims.add(time);

        List<Dimension> listForLon = new ArrayList<>();
        listForLon.add(lon);

        List<Dimension> listForLat = new ArrayList<>();
        listForLat.add(lat);

        List<Dimension> listForDepth = new ArrayList<>();
        listForDepth.add(depth);

        Variable dataVariable = dataFile.addVariable(null, "Var1", DataType.FLOAT, dims);
        Variable dataVariable2 = dataFile.addVariable(null, "Var2", DataType.FLOAT, dims);
        Variable varLon = dataFile.addVariable(null, ClassForCostants.lonVarName, DataType.FLOAT, listForLon);
        Variable varLat = dataFile.addVariable(null, ClassForCostants.latVarName, DataType.FLOAT, listForLat);
        Variable varDepth = dataFile.addVariable(null, ClassForCostants.depthVarName, DataType.FLOAT, listForDepth);

        dataFile.create();

        return;
    }

    private static void createNetcdfFileWithDifferentDimSizes(String filePath) throws IOException {

        final int NLAT = 6;
        final int NLON = 12;
        final int NDEPTH = 10;

        NetcdfFileWriter dataFile = null;


        dataFile = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, filePath);

        Dimension lat = dataFile.addDimension(null, ClassForCostants.latVarName, NLAT);
        Dimension lon = dataFile.addDimension(null, ClassForCostants.lonVarName, NLON);
        Dimension depth1 = dataFile.addDimension(null, ClassForCostants.depthVarName, NDEPTH);
        Dimension depth2 = dataFile.addDimension(null, ClassForCostants.depthVarName + "2", NDEPTH - 1);
        Dimension time = dataFile.addDimension(null, ClassForCostants.timeVarName, 1);

        List<Dimension> dims = new ArrayList<>();
        dims.add(lat);
        dims.add(lon);
        dims.add(depth1);
        dims.add(time);

        List<Dimension> listForLon = new ArrayList<>();
        listForLon.add(lon);

        List<Dimension> listForLat = new ArrayList<>();
        listForLat.add(lat);

        List<Dimension> listForDepth = new ArrayList<>();
        listForDepth.add(depth2);

        Variable dataVariable = dataFile.addVariable(null, "Var1", DataType.FLOAT, dims);
        Variable dataVariable2 = dataFile.addVariable(null, "Var2", DataType.FLOAT, dims);
        Variable varLon = dataFile.addVariable(null, ClassForCostants.lonVarName, DataType.FLOAT, listForLon);
        Variable varLat = dataFile.addVariable(null, ClassForCostants.latVarName, DataType.FLOAT, listForLat);
        Variable varDepth = dataFile.addVariable(null, ClassForCostants.depthVarName, DataType.FLOAT, listForDepth);

        dataFile.create();

        return;
    }


}
