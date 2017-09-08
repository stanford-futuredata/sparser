package edu.stanford.sparser;

public class SparserNative {
    static {
        System.loadLibrary("sparser"); // Load native library at runtime
    }

    // Declare a native method parse() that receives nothing and returns void
    public native long parse(String filename, long addr, long start, long length,
                             long recordSize, long maxRecords);
}
