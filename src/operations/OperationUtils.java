package operations;

public class OperationUtils {
    private static boolean isUnix() {
        String os = System.getProperty("os.name").toLowerCase();
        return (os.indexOf("nix") >= 0 || os.indexOf("nux") >= 0 || os.indexOf("aix") >= 0);
    }

    public static String dbPath(String columnDB){
        String path;
        if(isUnix()) {
            path = "/tmp/";
        } else {
            path = "C:\\Windows\\Temp\\";
        }
        return path + columnDB + ".minibase-db";
    }

}