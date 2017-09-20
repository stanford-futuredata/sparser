package edu.stanford.sparser;

public class SparserNative {
    static {
        System.loadLibrary("sparser"); // Load native library at runtime
        init();
    }

    // Declare a native method parse() that receives nothing and returns the
    // number of records parsed
    public native long parse(String filename, int filename_length, long addr, long start,
                              long length, long recordSize, long maxRecords);
    private static native void init();
}
