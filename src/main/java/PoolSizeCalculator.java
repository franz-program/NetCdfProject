import java.io.File;
import java.util.Arrays;
import java.util.List;

public class PoolSizeCalculator {

    private static int maxHeapSizeInMB = 4100;
    private static double heapForDataRatio = 0.7;

    private static int unpackingDimFactorNetcdfFile = 8;

    private static String LITTLE_AVAILABLE_MEMORY_MSG = "It appears the maximum heap size isn't high compared to" +
            "file sizes. One file at a time will be processed";
    private static LogLevel LITTLE_AVAILABLE_MEMORY_LOG_LVL = LogLevel.WARNING;


    public static int getPoolSize(List<String> filePaths, InfoLogger infoLogger) {

        long thirdQuartileFileDim = getThirdQuartile(filePaths.stream().mapToLong(f -> new File(f).length()).toArray());

        int heapMBForData = (int) (maxHeapSizeInMB * heapForDataRatio);

        int estimateOfPoolDim = getEstimateOfPoolDim(heapMBForData, thirdQuartileFileDim);

        if (estimateOfPoolDim == 0) {
            infoLogger.log(LITTLE_AVAILABLE_MEMORY_MSG, LITTLE_AVAILABLE_MEMORY_LOG_LVL);
            estimateOfPoolDim = 1;
        }

        if(Runtime.getRuntime().availableProcessors() == 1)
            return 1;

        return Math.min(estimateOfPoolDim, Runtime.getRuntime().availableProcessors() - 1);
    }

    private static int getEstimateOfPoolDim(int heapMBForData, long thirdQuartileFileDim) {
        double thirdQuartileFileDimMB = ((double) thirdQuartileFileDim) / 1000000.0;
        return (int) ((double) heapMBForData / (double) (thirdQuartileFileDimMB * unpackingDimFactorNetcdfFile * 2));
    }

    private static long getThirdQuartile(long[] array) {
        Arrays.sort(array);
        int positionThirdQuartile = (int) (array.length * 0.75);
        return array[positionThirdQuartile];
    }


}
