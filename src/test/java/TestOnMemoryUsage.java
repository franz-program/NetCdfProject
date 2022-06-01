import ucar.ma2.ArrayFloat;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class TestOnMemoryUsage {

    public static void main(String[] args) throws IOException {


        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
        }

        String filepath = "D:\\UNIVERSITA'\\TESI\\dati su cui lavorare\\O2o aperti\\ave.20150111-12_00_00.O2o.nc";
        String varName = "O2o";


        NetcdfFile netcdfFile = NetcdfUtilityFunctions.openFile(filepath);
        Variable variable = netcdfFile.findVariable(varName);
        int[] sizes = variable.getShape();

        DataReceiver dataReceiver = new DataCollector(sizes[1] * sizes[2] * sizes[3]);


        ArrayFloat.D4 data = (ArrayFloat.D4) variable.read();

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
        }

        for (int i = 0; i < sizes[1]; i++)
            for (int j = 0; j < sizes[2]; j++)
                for (int k = 0; k < sizes[3]; k++)
                    dataReceiver.sendRow(new float[]{i, j, k, data.get(0, i, j, k)});

        System.out.println("finito l'invio");


        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
        }

        netcdfFile.close();
        data = null;
        System.gc();


        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
        }

        DataGetter dataGetter = dataReceiver.getDataGetter();
        System.out.println(Arrays.toString(dataGetter.getNextRow()));
        dataGetter = null;

        System.gc();
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
        }


    }

}
