import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;
import ucar.nc2.Variable;

import java.io.IOException;

public class NetcdfUtilityFunctions {

    private static int openingTries = 5;
    private static int openingWaitingTimeBetweenTriesInms = 5000;

    private static int closingTries = 5;
    private static int closingWaitingTimeBetweenTriesInms = 5000;

    public static float getFillValue(Variable v) {
        //TODO: da fare
        return 0;
    }

    public static NetcdfFile openFile(String fileFullPath) throws IOException {
        IOException possibleException = new IOException();
        for (int count = 0; count < openingTries; count++) {
            try {
                return NetcdfFiles.open(fileFullPath);
            } catch (IOException e) {
                possibleException = e;
            }

            try{
                Thread.sleep(openingWaitingTimeBetweenTriesInms);
            } catch (InterruptedException e) {
            }

        }
        throw possibleException;
    }

    public static void closeFile(NetcdfFile netcdfFile) throws IOException {
        IOException possibleException = new IOException();
        for (int count = 0; count < closingTries; count++) {
            try {
                netcdfFile.close();
                return;
            } catch (IOException e) {
                possibleException = e;
            }

            try{
                Thread.sleep(closingWaitingTimeBetweenTriesInms);
            } catch (InterruptedException e) {
            }

        }
        throw possibleException;
    }

}
