package com.kidneybone.snapshot.blocks;

import static com.kidneybone.snapshot.blocks.BlockUtils.isValidHash;
import static com.kidneybone.snapshot.blocks.BlockUtils.readCString;
import static com.kidneybone.snapshot.blocks.BlockUtils.readHash;
import static com.kidneybone.snapshot.blocks.BlockUtils.utf8Encode;
import static com.kidneybone.snapshot.blocks.BlockUtils.writeCString;
import static com.kidneybone.snapshot.blocks.BlockUtils.writeHash;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class CommitDataBlock extends BasicBlock {
    public static final int FILE_SIZE_BYTES = 8;
    public static final int PATH_SIZE_BYTES = 4096;
    public static final int ENTRIES_PER_BLOCK =
            (BLOCK_SIZE_BYTES - HASH_SIZE_BYTES) /
                (PATH_SIZE_BYTES + FILE_SIZE_BYTES + HASH_SIZE_BYTES);

    private ArrayList<String> _paths = new ArrayList<String>();
    private ArrayList<Long> _sizes = new ArrayList<Long>();
    private ArrayList<String> _pointers = new ArrayList<String>();
    private String _nextBlock;

    public CommitDataBlock() {
        _nextBlock = EMPTY_HASH;
    }

    public CommitDataBlock(String nextBlock) {
        _nextBlock = nextBlock;
    }

    public String getEntryPath(int i) {
        return _paths.get(i);
    }

    public long getEntrySize(int i) {
        return _sizes.get(i);
    }

    public String getEntryFilePointer(int i) {
        return _pointers.get(i);
    }

    public String getNextPointer() {
        return _nextBlock;
    }

    public int size() {
        return _paths.size();
    }

    public boolean isFull() {
        return _paths.size() == ENTRIES_PER_BLOCK;
    }

    public void registerFile(String path, long size, String pointer) {
        if (_paths.size() == ENTRIES_PER_BLOCK) {
            throw new IllegalStateException("Cannot add more than " + ENTRIES_PER_BLOCK + " to a single commit data block");
        }

        if (!isValidHash(pointer)) {
            throw new IllegalArgumentException("Cannot store pointer, is not valid SHA256 hash");
        }

        if (size < 0) {
            throw new IllegalArgumentException("Cannot store file size, must be non-negative");
        }

        byte[] encodedName = utf8Encode(path);
        if (encodedName.length > PATH_SIZE_BYTES) {
            throw new IllegalArgumentException("Cannot store path, requires more than " + PATH_SIZE_BYTES + " bytes to store");
        }

        _paths.add(path);
        _sizes.add(size);
        _pointers.add(pointer);
    }

    @Override
    protected void toBuffer(ByteBuffer buffer) {
        byte[] emptyFilename = new byte[PATH_SIZE_BYTES];
        for (int i = 0; i < ENTRIES_PER_BLOCK; i++) {
            if (i < _paths.size()) {
                writeCString(buffer, _paths.get(i), PATH_SIZE_BYTES);
                buffer.putLong(_sizes.get(i));
                writeHash(buffer, _pointers.get(i));
            } else {
                buffer.put(emptyFilename);
                buffer.putLong(0);
                writeHash(buffer, EMPTY_HASH);
            }
        }
        writeHash(buffer, _nextBlock);
    }

    @Override
    protected void fromBuffer(ByteBuffer buffer) {
        for (int i = 0; i < ENTRIES_PER_BLOCK; i++) {
            String path = readCString(buffer, PATH_SIZE_BYTES);
            long size = buffer.getLong();
            String pointer = readHash(buffer);

            if (!path.equals("")) {
                _paths.add(path);
                _sizes.add(size);
                _pointers.add(pointer);
            }
        }

        _nextBlock = readHash(buffer);
    }
}
