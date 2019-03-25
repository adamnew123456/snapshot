package com.kidneybone.snapshot.blocks;

import java.nio.ByteBuffer;

public class DataBlock extends BasicBlock {
    private byte[] _contents;

    public DataBlock() {
        _contents = new byte[BLOCK_SIZE_BYTES];
    }

    public DataBlock(ByteBuffer buffer) {
        _contents = new byte[BLOCK_SIZE_BYTES];
        buffer.get(_contents);
    }

    public long writeContent(ByteBuffer buffer, long maxSize) {
        long bytesRead = Math.min(maxSize, BLOCK_SIZE_BYTES);
        buffer.put(_contents, 0, (int) bytesRead);
        return bytesRead;
    }

    @Override
    protected void toBuffer(ByteBuffer buffer) {
        buffer.put(_contents);
    }

    @Override
    protected void fromBuffer(ByteBuffer buffer) {
        buffer.get(_contents);
    }
}
