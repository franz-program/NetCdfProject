public class NetcdfRowsSenderCreator {

    private NetcdfRowsSenderCreator(){
    }

    public static Runnable create(NetcdfRowsManager tableWriter, String[] columnsNames, String netcdfFileName,
                         InfoLogger infoLogger){
        return new NetcdfRowsSender(tableWriter, columnsNames, netcdfFileName, infoLogger);
    }

}
