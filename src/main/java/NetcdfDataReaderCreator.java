public class NetcdfDataReaderCreator {

    private NetcdfDataReaderCreator() {
    }

    public static Runnable create(AsynchronousOutputFileWriter outputFileWriter, String[] columnsNames, String netcdfFileFullPath,
                                  InfoLogger infoLogger, AsynchronousFilesValidityCollector filesValidityCollector,
                                  AsynchronousFailedFilesManager failedFilesManager, HeaderFileWriter headerFileWriter) {
        return new NetcdfDataReader(outputFileWriter, columnsNames, netcdfFileFullPath, infoLogger,
                filesValidityCollector, failedFilesManager, headerFileWriter);
    }

}
