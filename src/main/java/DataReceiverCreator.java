public class DataReceiverCreator {

    private DataReceiverCreator(){
    }

    public static DataReceiver createDataReceiver(int nOfRows){
        return (DataReceiver) new DataCollector(nOfRows);
    }

}
