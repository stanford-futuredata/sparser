package edu.stanford.sparser;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;

class UnsafeAccess {
    private static Unsafe UNSAFE;
    private static long addressOffset;

    static {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            UNSAFE = (Unsafe) theUnsafe.get(null);
            addressOffset = UNSAFE.objectFieldOffset(Buffer.class.getDeclaredField("address"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // buf must be allocated off-heap (e.g., using allocateDirect, or a memory-mapped file)
    static long getRawPointer(ByteBuffer buf) {
        return UnsafeAccess.UNSAFE.getLong(buf, addressOffset);
    }
}
