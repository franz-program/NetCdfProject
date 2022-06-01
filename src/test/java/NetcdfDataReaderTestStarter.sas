import java.io.File;
import java.io.IOException;

public class NetcdfDataReaderTestStarter {

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

        NetcdfDataReaderTester fileWriter = new NetcdfDataReaderTester(fileFullPath, columnNames);

        new Thread(fileWriter).start();
        new Thread(NetcdfDataReaderCreator.create(fileWriter, columnNames, fileFullPath, uselessInfoLogger, fileWriter, fileWriter, fileWriter)).start();

    }

}
