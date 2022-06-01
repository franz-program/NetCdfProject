import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MyCsvWriter {

    private String filepath;
    private PrintWriter writer;

    private MyCsvWriter(String filepath){
        this.filepath = filepath;
    }

    public static MyCsvWriter createAndOpenPrinter(String filepath, String[] header) throws IOException {
        MyCsvWriter csvWriter = new MyCsvWriter(filepath);
        csvWriter.open();
        csvWriter.writer.println(csvWriter.convertToCSV(header));
        return csvWriter;
    }

    private String convertToCSV(String[] data) {
        return Stream.of(data)
                .map(this::escapeSpecialCharacters)
                .collect(Collectors.joining(","));
    }

    private String escapeSpecialCharacters(String data) {
        String escapedData = data.replaceAll("\\R", " ");
        if (data.contains(",") || data.contains("\"") || data.contains("'")) {
            data = data.replace("\"", "\"\"");
            escapedData = "\"" + data + "\"";
        }
        return escapedData;
    }

    public void write(Object[] row) throws IOException {
        writer.println(convertToCSV(
                Arrays.stream(row).map(String::valueOf).toArray(String[]::new)
                ));
    }

    public void open() throws FileNotFoundException {
        writer = new PrintWriter(filepath);
    }

    public void flush() throws IOException {
        writer.flush();
    }

    public void close() throws IOException {
        writer.flush();
        writer.close();
    }



}
