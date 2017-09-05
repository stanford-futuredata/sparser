import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;


public class SparserSpark {
    static {
        System.loadLibrary("sparser"); // Load native library at runtime
    }

    public static final long NUM_BYTES = 512;

    // Declare a native method parse() that receives nothing and returns void
    private native void parse(String filename, long addr, long size);

    public static void main(String[] args) {
        String jsonFilename = "/lfs/0/fabuzaid/sparser/benchmarks/datagen/_data/tweets-small.json"; // Should be some file on your system
        SparkConf conf = new SparkConf().setAppName("Sparser Spark");
        JavaSparkContext sc = new JavaSparkContext(conf);

        ByteBuffer buf = ByteBuffer.allocateDirect((int) NUM_BYTES);
        buf.order(ByteOrder.nativeOrder());
        final long rawAddress = UnsafeAccess.getRawPointer(buf);
        System.out.println("In Java, the address is " + String.format("0x%08x", rawAddress));
        new SparserSpark().parse(jsonFilename, rawAddress, NUM_BYTES); // invoke the native method

        UnsafeRow offheapUnsafeRow = new UnsafeRow(1);
        for (long i = 0; i < NUM_BYTES/4; ++i) {
            offheapUnsafeRow.pointTo(
                    null,
                    rawAddress - 8 + i*4, // TODO: figure out why UnsafeRow is 8 bytes ahead of where it should be
                    4);
            int unsafeRowInt = offheapUnsafeRow.getInt(0);
            System.out.println("unsafeRowInt: " + unsafeRowInt);
        }
        sc.stop();
    }
}
