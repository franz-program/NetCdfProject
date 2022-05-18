import org.checkerframework.checker.units.qual.C;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.Index4D;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class NetcdfTestFilesCreator {

    public final static String var1TestName = "Var1";
    public final static String var2TestName = "Var2";

    public final static int TIME_DIM_LENGTH = 1;
    public final static int DEPTH_DIM_LENGTH = 5;
    public final static int LAT_DIM_LENGTH = 6;
    public final static int LON_DIM_LENGTH = 12;

    public static void create(String fileFullPath, int fileUniqueNumberForGeneratingValues) throws IOException {

        try (NetcdfFileWriter dataFile = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, fileFullPath)) {

            List<Dimension> dims = addDimensionsToFileAndReturnList(dataFile);

            Variable vlat = dataFile.addVariable(null, ClassForCostants.latVarName, DataType.FLOAT, ClassForCostants.latVarName);
            Variable vlon = dataFile.addVariable(null, ClassForCostants.lonVarName, DataType.FLOAT, ClassForCostants.lonVarName);
            Variable vdepth = dataFile.addVariable(null, ClassForCostants.depthVarName, DataType.FLOAT, ClassForCostants.depthVarName);
            Variable var1 = dataFile.addVariable(null, var1TestName, DataType.FLOAT, dims);
            Variable var2 = dataFile.addVariable(null, var2TestName, DataType.FLOAT, dims);

            dataFile.create();

            Array dataLat = Array.factory(DataType.FLOAT, new int[]{LAT_DIM_LENGTH});
            Array dataLon = Array.factory(DataType.FLOAT, new int[]{LON_DIM_LENGTH});
            Array dataDepth = Array.factory(DataType.FLOAT, new int[]{DEPTH_DIM_LENGTH});

            setValuesForFloat1DArray(dataLat, LAT_DIM_LENGTH);
            setValuesForFloat1DArray(dataLon, LON_DIM_LENGTH);
            setValuesForFloat1DArray(dataDepth, DEPTH_DIM_LENGTH);

            try {
                dataFile.write(vlat, dataLat);
                dataFile.write(vlon, dataLon);
                dataFile.write(vdepth, dataDepth);
            } catch (InvalidRangeException e) {
                e.printStackTrace();
            }


            int[] iDim = new int[]{TIME_DIM_LENGTH, DEPTH_DIM_LENGTH, LAT_DIM_LENGTH, LON_DIM_LENGTH};
            Array var1Data = Array.factory(DataType.FLOAT, iDim);
            Array var2Data = Array.factory(DataType.FLOAT, iDim);

            Index4D idx = new Index4D(iDim);

            for (int i = 0; i < DEPTH_DIM_LENGTH; i++)
                for (int j = 0; j < LAT_DIM_LENGTH; j++)
                    for (int k = 0; k < LON_DIM_LENGTH; k++) {
                        idx.set(0, i, j, k);
                        var1Data.setFloat(idx, generateValuesForColumns(j, k, i, 1, fileUniqueNumberForGeneratingValues));
                        var2Data.setFloat(idx, generateValuesForColumns(j, k, i, 2, fileUniqueNumberForGeneratingValues));
                    }

            try {
                dataFile.write(var1, var1Data);
                dataFile.write(var2, var2Data);
            } catch (InvalidRangeException e) {
                throw new RuntimeException("Internal error: " + e);
            }


        }

    }

    private static List<Dimension> addDimensionsToFileAndReturnList(NetcdfFileWriter netcdfFile){
        List<Dimension> dims = new ArrayList<>();
        dims.add(netcdfFile.addDimension(null, ClassForCostants.timeVarName, TIME_DIM_LENGTH));
        dims.add(netcdfFile.addDimension(null, ClassForCostants.depthVarName, DEPTH_DIM_LENGTH));
        dims.add(netcdfFile.addDimension(null, ClassForCostants.latVarName, LAT_DIM_LENGTH));
        dims.add(netcdfFile.addDimension(null, ClassForCostants.lonVarName, LON_DIM_LENGTH));

        return dims;
    }

    private static void setValuesForFloat1DArray(Array array, int len){
        for(int i = 0; i < len; i++)
            array.setFloat(i, i);
    }

    public static int generateValuesForColumns(int latValue, int lonValue, int depthValue, int varCode, int fileUniqueValue) {
        return Arrays.hashCode(new int[]{latValue, lonValue, depthValue, varCode, fileUniqueValue});
    }

    public static String[] getColumnNames(){
        return new String[]{ClassForCostants.timeVarName, ClassForCostants.depthVarName,
                ClassForCostants.latVarName, ClassForCostants.lonVarName, var1TestName, var2TestName};
    }

}
