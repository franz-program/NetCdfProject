import ucar.nc2.NetcdfFile;
import ucar.nc2.ffi.netcdf.NetcdfClibrary;

import java.util.ArrayList;
import java.util.List;

public class Main {

    //TODO: guarda quali cose devono generare eccezioni per evitare che il programma continui

    private static final String logTxtFileName;
    private static final String logTxtFileDirectoryName;

    public static void main(String[] args) {

        List<String> ncFilesNames = getNcFilesNames();

        List<InfoLogger> loggers = instantiateLoggers();
        InfoLogger logCollector = new InfoLoggerCollector(loggers);


    }

    private static List<InfoLogger> instantiateLoggers(){
        List<InfoLogger> loggers = new ArrayList<>();
        loggers.add(new InfoLoggerOnTxtFile(logTxtFileName, logTxtFileDirectoryName));
        loggers.add(new InfoLoggerOnStdOutput(LogLevel.WARNING));

        return loggers;
    }

    private static List<String> getNcFilesNames(){

    }

}
