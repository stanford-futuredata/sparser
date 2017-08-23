import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

public class SparserSpark {
  static {
    System.loadLibrary("sparser"); // Load native library at runtime
  }

  // Declare a native method sayHello() that receives nothing and returns void
  private native void sayHello();

  public static void main(String[] args) {
    // String logFile = "/lfs/0/fabuzaid/spark/README.md"; // Should be some file on your system
    SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
    // Dataset<String> logData = spark.read().textFile(logFile).cache();

    // long numAs = logData.filter(s -> s.contains("a")).count();
    // long numBs = logData.filter(s -> s.contains("b")).count();

    // System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
    new SparserSpark().sayHello();  // invoke the native method
    spark.stop();
  }

}
