import java.util.Arrays;

public class RowsGeneratorForTest implements Runnable {

    private int runnableNumber;
    private int nOfRows;
    private NetcdfRowsManager netcdfRowsManager;

    public RowsGeneratorForTest(NetcdfRowsManager netcdfRowsManager, int runnableNumber, int nOfRows){
        this.netcdfRowsManager = netcdfRowsManager;
        this.nOfRows = nOfRows;
        this.runnableNumber = runnableNumber;
    }

    @Override
    public void run() {
        for (int j = 0; j < nOfRows; j++) {
            Object[] row = new Object[]{runnableNumber, j, Arrays.hashCode(new int[]{runnableNumber, j})};
            netcdfRowsManager.addRow(row);
        }
        netcdfRowsManager.sourceHasFinished(String.valueOf(runnableNumber));
    }

}
