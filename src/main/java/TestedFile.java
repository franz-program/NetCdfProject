public class TestedFile {

    //TODO: pensa ad un modo migliore per ottene la stessa cosa

    private String fileName;
    private boolean testPassed;

    public TestedFile(String fileName, boolean testPassed){
        this.fileName = fileName;
        this.testPassed = testPassed;
    }

    public String getFileName(){
        return fileName;
    }

    public boolean isTestPassed(){
        return testPassed;
    }

}
