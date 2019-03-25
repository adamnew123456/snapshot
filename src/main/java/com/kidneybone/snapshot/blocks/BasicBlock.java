package com.kidneybone.snapshot.blocks;

import java.nio.ByteBuffer;

/**
 * A block is the basic unit of storage within the block pool. This contains
 * the methods which will be useful to all kinds of blocks regardless of
 * their contents.
 */
public abstract class BasicBlock {
    public static final int BLOCK_SIZE_BYTES = 64 * 1024;
    public static final String EMPTY_HASH = "0000000000000000000000000000000000000000000000000000000000000000";

    // The 256-bit hash can be represented in 32 binary bytes, or 64 ASCII
    // bytes since each byte consists of two characters
    public static final int HASH_SIZE_BYTES = 64;

    protected abstract void toBuffer(ByteBuffer buffer);
    protected abstract void fromBuffer(ByteBuffer buffer);

    /**
     * Stores the content of the block in the buffer at the current position.
     */
    public void serialize(ByteBuffer buffer) {
        int start = buffer.position();
        toBuffer(buffer);
        int end = buffer.position();

        int wrote = end - start;
        if (wrote < BLOCK_SIZE_BYTES) {
            buffer.put(new byte[BLOCK_SIZE_BYTES - wrote]);
        }
    }

    /**
     * Initializes the block from the buffer at the current position.
     *
     * Note that this should only ever be done once; if you need to
     * unserialize multiple blocks (especially in a loop), you should
     * allocate a new block each time.
     */
    public void unserialize(ByteBuffer buffer) {
        int start = buffer.position();
        fromBuffer(buffer);
        int end = buffer.position();

        int read = end - start;
        if (read < BLOCK_SIZE_BYTES) {
            buffer.position(buffer.position() + BLOCK_SIZE_BYTES - read);
        }
    }
}
