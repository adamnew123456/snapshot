package com.kidneybone.snapshot.blocks;

import static com.kidneybone.snapshot.blocks.BlockUtils.readHash;
import static com.kidneybone.snapshot.blocks.BlockUtils.writeHash;

import java.nio.ByteBuffer;

/*
 * Although there are more compact ways of storing commits, we would have to
 * have a more complicated addressing scheme (for example, hash + index instead
 * of just a hash) in order to identify them.
 */

public class CommitBlock extends BasicBlock {
    private long _timestamp;
    private String _dataPointer;
    private String _prevCommit;

    public CommitBlock() {
        _timestamp = 0;
        _dataPointer = EMPTY_HASH;
        _prevCommit = EMPTY_HASH;
    }

    public CommitBlock(long timestamp, String dataPointer, String prevCommit) {
        _timestamp = timestamp;
        _dataPointer = dataPointer;
        _prevCommit = prevCommit;
    }

    public long getTimestamp() {
        return _timestamp;
    }

    public String getDataPointer() {
        return _dataPointer;
    }

    public String getPreviousCommit() {
        return _prevCommit;
    }

    @Override
    protected void toBuffer(ByteBuffer buffer) {
        buffer.putLong(_timestamp);
        writeHash(buffer, _dataPointer);
        writeHash(buffer, _prevCommit);
    }

    @Override
    protected void fromBuffer(ByteBuffer buffer) {
        _timestamp = buffer.getLong();
        _dataPointer = readHash(buffer);
        _prevCommit = readHash(buffer);
    }
}
