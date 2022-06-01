import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class TestForCsvSizeOnDisk {

    private static final int nOfRows = 50000000;
    private static final int nOfColumns = 5;
    private static final String testFileFullPath = "D:\\UNIVERSITA'\\TESI\\generated files folder\\testFile.csv";

    public static void main(String[] args) throws IOException {
        CSVPrinter printer = CSVFormat.DEFAULT.withHeader("sas").print(Files.newBufferedWriter(
                Paths.get(testFileFullPath)
        ));

        for(int i = 0; i < nOfRows; i++) {
            List<Object> row = new ArrayList<>();
            for (int j = 0; j < nOfColumns; j++) {
                row.add(j);
            }
            printer.printRecord(row);
        }

        printer.close();

    }

}
