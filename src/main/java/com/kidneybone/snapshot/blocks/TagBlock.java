package com.kidneybone.snapshot.blocks;

import static com.kidneybone.snapshot.blocks.BlockUtils.isValidHash;
import static com.kidneybone.snapshot.blocks.BlockUtils.readCString;
import static com.kidneybone.snapshot.blocks.BlockUtils.readHash;
import static com.kidneybone.snapshot.blocks.BlockUtils.utf8Encode;
import static com.kidneybone.snapshot.blocks.BlockUtils.writeCString;
import static com.kidneybone.snapshot.blocks.BlockUtils.writeHash;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class TagBlock extends BasicBlock {
    public static final int ENCODED_TAG_SIZE = HASH_SIZE_BYTES;
    public static final int ENTRIES_PER_BLOCK = (BLOCK_SIZE_BYTES - HASH_SIZE_BYTES) / (HASH_SIZE_BYTES * 2);
    private ArrayList<String> _names = new ArrayList<String>();
    private ArrayList<String> _pointers = new ArrayList<String>();
    private String _nextBlock;

    public TagBlock() {
        _nextBlock = EMPTY_HASH;
    }

    public TagBlock(String nextBlock) {
        _nextBlock = nextBlock;
    }

    public String getEntryName(int i) {
        return _names.get(i);
    }

    public String getEntryCommit(int i) {
        return _pointers.get(i);
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

    public void registerTag(String name, String pointer) {
        if (_names.size() == ENTRIES_PER_BLOCK) {
            throw new IllegalStateException("Cannot add more than " + ENTRIES_PER_BLOCK + " to a single tag block");
        }

        if (!isValidHash(pointer)) {
            throw new IllegalArgumentException("Cannot store pointer, is not valid SHA256 hash");
        }

        byte[] encodedName = utf8Encode(name);
        if (encodedName.length > ENCODED_TAG_SIZE) {
            throw new IllegalArgumentException("Cannot store tag, requires more than 64 bytes to store");
        }

        _names.add(name);
        _pointers.add(pointer);
    }

    @Override
    protected void toBuffer(ByteBuffer buffer) {
        byte[] zeroBuffer = new byte[ENCODED_TAG_SIZE];
        for (int i = 0; i < ENTRIES_PER_BLOCK; i++) {
            if (i < _names.size()) {
                writeCString(buffer, _names.get(i), ENCODED_TAG_SIZE);
                writeHash(buffer, _pointers.get(i));
            } else {
                buffer.put(zeroBuffer);
                writeHash(buffer, EMPTY_HASH);
            }
        }

        writeHash(buffer, _nextBlock);
    }

    @Override
    protected void fromBuffer(ByteBuffer buffer) {
        for (int i = 0; i < ENTRIES_PER_BLOCK; i++) {
            String name = readCString(buffer, ENCODED_TAG_SIZE);
            String pointer = readHash(buffer);

            if (!name.equals("")) {
                _names.add(name);
                _pointers.add(pointer);
            }
        }

        _nextBlock = readHash(buffer);
    }
}
