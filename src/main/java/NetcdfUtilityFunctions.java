import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;
import ucar.nc2.Variable;

import java.io.IOException;

public class NetcdfUtilityFunctions {

    public static float getFillValue(Variable v) {
        //TODO: da fare
        return 0;
    }

    public static NetcdfFile openFile(String fileFullPath) throws IOException {
        return NetcdfFiles.open(fileFullPath);
    }

    public static void closeFile(NetcdfFile netcdfFile) throws IOException {
        netcdfFile.close();
    }

}
