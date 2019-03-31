package com.kidneybone.snapshot.blocks;

import static com.kidneybone.snapshot.blocks.BlockUtils.isEmptyHash;
import static com.kidneybone.snapshot.blocks.BlockUtils.isValidHash;
import static com.kidneybone.snapshot.blocks.BlockUtils.readHash;
import static com.kidneybone.snapshot.blocks.BlockUtils.writeHash;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class IndexBlock extends BasicBlock {
    public static final int ENTRIES_PER_BLOCK = (BLOCK_SIZE_BYTES - HASH_SIZE_BYTES) / (HASH_SIZE_BYTES * 3);
    private ArrayList<String> _pointers = new ArrayList<String>();
    private ArrayList<Long> _offsets = new ArrayList<Long>();
    private ArrayList<Integer> _sizes = new ArrayList<Integer>();
    private ArrayList<Boolean> _compressedStatus = new ArrayList<Boolean>();
    private String _nextBlock;

    public IndexBlock() {
        _nextBlock = EMPTY_HASH;
    }

    public IndexBlock(String nextBlock) {
        _nextBlock = nextBlock;
    }

    public String getEntryPointer(int i) {
        return _pointers.get(i);
    }

    public long getEntryOffset(int i) {
        return _offsets.get(i);
    }

    public int getEntrySize(int i) {
        return _sizes.get(i);
    }

    public boolean getEntryIsCompressed(int i) {
        return _compressedStatus.get(i);
    }

    public String getNextPointer() {
        return _nextBlock;
    }

    public int size() {
        return _pointers.size();
    }

    public boolean isFull() {
        return _pointers.size() == ENTRIES_PER_BLOCK;
    }

    public void registerBlock(String pointer, long offset, int size, boolean isCompressed) {
        if (_pointers.size() == ENTRIES_PER_BLOCK) {
            throw new IllegalStateException("Cannot add more than " + ENTRIES_PER_BLOCK + " to a single index block");
        }

        if (!isValidHash(pointer)) {
            throw new IllegalArgumentException("Cannot store pointer, is not valid SHA256 hash");
        }

        if (offset < 0) {
            throw new IllegalArgumentException("Cannot store offset, must be non-negative");
        }

        if (offset < 0) {
            throw new IllegalArgumentException("Cannot store size, must be non-negative");
        }

        _pointers.add(pointer);
        _offsets.add(offset);
        _sizes.add(size);
        _compressedStatus.add(isCompressed);
    }

    @Override
    protected void toBuffer(ByteBuffer buffer) {
        for (int i = 0; i < ENTRIES_PER_BLOCK; i++) {
            if (i < _pointers.size()) {
                writeHash(buffer, _pointers.get(i));
                buffer.putLong(_offsets.get(i));
                buffer.putInt(_sizes.get(i));
                buffer.putInt(_compressedStatus.get(i) ? 1 : 0);
            } else {
                writeHash(buffer, EMPTY_HASH);
                buffer.putLong(0);
                buffer.putInt(0);
                buffer.putInt(0);
            }
        }

        writeHash(buffer, _nextBlock);
    }

    @Override
    protected void fromBuffer(ByteBuffer buffer) {
        _pointers.clear();
        _offsets.clear();

        for (int i = 0; i < ENTRIES_PER_BLOCK; i++) {
            String pointer = readHash(buffer);
            long offset = buffer.getLong();
            int size = buffer.getInt();
            boolean isCompressed = buffer.getInt() == 1;

            if (!isEmptyHash(pointer)) {
                _pointers.add(pointer);
                _offsets.add(offset);
                _sizes.add(size);
                _compressedStatus.add(isCompressed);
            }
        }

        _nextBlock = readHash(buffer);
    }
}
