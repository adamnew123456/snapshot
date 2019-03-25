package com.kidneybone.snapshot.blocks;

import static com.kidneybone.snapshot.blocks.BlockUtils.isEmptyHash;
import static com.kidneybone.snapshot.blocks.BlockUtils.isValidHash;
import static com.kidneybone.snapshot.blocks.BlockUtils.readHash;
import static com.kidneybone.snapshot.blocks.BlockUtils.writeHash;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class FileBlock extends BasicBlock {
    public static final int ENTRIES_PER_BLOCK = (BLOCK_SIZE_BYTES - HASH_SIZE_BYTES) / HASH_SIZE_BYTES;
    private ArrayList<String> _pointers = new ArrayList<String>();
    private String _previousBlock = EMPTY_HASH;

    public FileBlock() {
        _previousBlock = EMPTY_HASH;
    }

    public FileBlock(String previousBlock) {
        _previousBlock = EMPTY_HASH;
    }

    public String getEntryPointer(int i) {
        return _pointers.get(i);
    }

    public String getPreviousPointer() {
        return _previousBlock;
    }

    public int size() {
        return _pointers.size();
    }

    public boolean isFull() {
        return _pointers.size() == ENTRIES_PER_BLOCK;
    }

    public void registerBlock(String pointer) {
        if (_pointers.size() == ENTRIES_PER_BLOCK) {
            throw new IllegalStateException("Cannot add more than " + ENTRIES_PER_BLOCK + " to a single index block");
        }

        if (!isValidHash(pointer)) {
            throw new IllegalArgumentException("Cannot store pointer, is not valid SHA256 hash");
        }

        _pointers.add(pointer);
    }

    @Override
    protected void toBuffer(ByteBuffer buffer) {
        for (int i = 0; i < ENTRIES_PER_BLOCK; i++) {
            if (i < _pointers.size()) {
                writeHash(buffer, _pointers.get(i));
            } else {
                writeHash(buffer, EMPTY_HASH);
            }
        }

        writeHash(buffer, _previousBlock);
    }

    @Override
    protected void fromBuffer(ByteBuffer buffer) {
        _pointers.clear();

        for (int i = 0; i < ENTRIES_PER_BLOCK; i++) {
            String pointer = readHash(buffer);
            if (!isEmptyHash(pointer)) {
                _pointers.add(pointer);
            }
        }

        _previousBlock = readHash(buffer);
    }
}
