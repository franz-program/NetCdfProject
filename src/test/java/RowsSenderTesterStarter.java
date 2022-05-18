import java.io.File;
import java.io.IOException;

public class RowsSenderTesterStarter {

    public static void main(String[] args) {
        UselessInfoLogger uselessInfoLogger = new UselessInfoLogger();

        String[] columnNames = NetcdfTestFilesCreator.getColumnNames();

        String fileFullPath;
        if (args.length <= 0)
            throw new IllegalArgumentException("Put any folder's path for the test file, as the first argument (no last separator needed)");

        fileFullPath = args[0] + File.separator + "rowsSenderTestFile.nc";

        try {
            NetcdfTestFilesCreator.create(fileFullPath, 0);
        } catch (IOException e) {
            System.out.println("Could not create test file, test won't proceed");
            return;
        }

        NetcdfRowsManager netcdfRowsManager = new RowsSenderTester(fileFullPath);
        netcdfRowsManager.setColumnNames(columnNames);

        new Thread(netcdfRowsManager).start();
        new Thread(NetcdfRowsSenderCreator.create(netcdfRowsManager, columnNames, fileFullPath, uselessInfoLogger)).start();

    }

}
