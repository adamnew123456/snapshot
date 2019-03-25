package com.kidneybone.snapshot;

import static com.kidneybone.snapshot.blocks.BlockUtils.readHash;
import static com.kidneybone.snapshot.blocks.BlockUtils.writeHash;

import java.nio.ByteBuffer;

import com.kidneybone.snapshot.blocks.BasicBlock;
import com.kidneybone.snapshot.blocks.IndexBlock;
import com.kidneybone.snapshot.blocks.TagBlock;

public class HeaderBlock {
    public static final int HEADER_SIZE_BYTES = BasicBlock.HASH_SIZE_BYTES + BasicBlock.BLOCK_SIZE_BYTES * 2;

    private String _lastCommit = BasicBlock.EMPTY_HASH;
    private IndexBlock _rootIndex = new IndexBlock();
    private TagBlock _rootTags = new TagBlock();

    public IndexBlock getRootIndex() {
        return _rootIndex;
    }

    public String getLastCommit() {
        return _lastCommit;
    }

    public void setLastCommit(String value) {
        _lastCommit = value;
    }

    /**
     * Replaces the current root index with a new, empty index block pointing
     * to the given hash.
     */
    public IndexBlock linkInNewIndexBlock(String oldIndexHash) {
        _rootIndex = new IndexBlock(oldIndexHash);
        return _rootIndex;
    }

    public TagBlock getRootTags() {
        return _rootTags;
    }

    /**
     * Replaces the current root tags with a new, empty tag block pointing
     * to the given hash.
     */
    public TagBlock linkInNewTagBlock(String oldTagHash) {
        _rootTags = new TagBlock(oldTagHash);
        return _rootTags;
    }

    public void serialize(ByteBuffer buffer) {
        writeHash(buffer, _lastCommit);
        _rootIndex.toBuffer(buffer);
        _rootTags.toBuffer(buffer);
    }

    public void unserialize(ByteBuffer buffer) {
        _lastCommit = readHash(buffer);
        _rootIndex.fromBuffer(buffer);
        _rootTags.fromBuffer(buffer);
    }
}
