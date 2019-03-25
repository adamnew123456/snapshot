package com.kidneybone.snapshot;

import static com.kidneybone.snapshot.blocks.BlockUtils.hashOfLastBlock;
import static com.kidneybone.snapshot.blocks.BlockUtils.isEmptyHash;
import static com.kidneybone.snapshot.blocks.BlockUtils.isValidHash;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Date;
import java.util.HashMap;

import com.kidneybone.snapshot.blocks.BasicBlock;
import com.kidneybone.snapshot.blocks.CommitBlock;
import com.kidneybone.snapshot.blocks.IndexBlock;
import com.kidneybone.snapshot.blocks.TagBlock;

public class BlockStore {
    private static long HASH_IS_EMPTY = -1;
    private static long HASH_NOT_FOUND = -2;

    private FileChannel _channel;
    private HeaderBlock _header = new HeaderBlock();
    private HashMap<String, Long> _indexCache = new HashMap<>();

    public BlockStore(FileChannel channel) {
        _channel = channel;
    }

    /**
     * Gets the current header block.
     */
    public HeaderBlock getHeader() {
        return _header;
    }

    /**
     * Gets a buffer big enough to hold a header.
     */
    private ByteBuffer newHeaderBuffer() {
        return ByteBuffer.allocate(HeaderBlock.HEADER_SIZE_BYTES);
    }

    /**
     * Gets a buffer big enough to hold a non-header block.
     */
    private ByteBuffer newBlockBuffer() {
        return ByteBuffer.allocate(BasicBlock.BLOCK_SIZE_BYTES);
    }

    /**
     * Flushes the current header to the start of the channel.
     */
    public void serialize() throws IOException {
        ByteBuffer headerBuffer = newHeaderBuffer();
        _header.serialize(headerBuffer);

        headerBuffer.position(0);
        _channel.write(headerBuffer, 0);
        _channel.force(true);
    }

    /**
     * Initializes the header from the start of the channel.
     */
    public void unserialize() throws IOException {
        ByteBuffer headerBuffer = newHeaderBuffer();
        _channel.position(0);
        _channel.read(headerBuffer, 0);

        headerBuffer.position(0);
        _header.unserialize(headerBuffer);
    }

    /**
     * Creates a new commit from the data in the given directory, and stores it.
     */
    public void addCommit(String baseDirectory) throws IOException {
        Commit commit = new Commit(this);
        String dataHash = commit.storeFileTree(baseDirectory);

        String lastCommit = _header.getLastCommit();
        CommitBlock commitBlock = new CommitBlock(new Date().getTime(), dataHash, lastCommit);

        _header.setLastCommit(serializeBlock(commitBlock));
    }

    /**
     * Restores the contents of a commit into the given directory.
     */
    public void restoreCommit(String baseDirectory, String commitPointer) throws IOException {
        Commit commit = new Commit(this);

        CommitBlock commitBlock = new CommitBlock();
        unserializeBlock(commitBlock, commitPointer);

        commit.restoreFileTree(baseDirectory, commitBlock.getDataPointer());
    }

    /**
     * Creates a tag that points to the given hash.
     */
    public void addTag(String tagName, String hash) throws IOException {
        tagName = tagName.trim();
        if (tagName.equals("")) {
            throw new IllegalArgumentException("Tags cannot be empty or consist of only whitespace");
        }

        long offset = getOffsetForHash(hash.toUpperCase());
        if (offset == HASH_IS_EMPTY || offset == HASH_NOT_FOUND) {
            throw new IllegalArgumentException("The hash " + hash + " does not refer to a block");
        }

        TagBlock rootTags = _header.getRootTags();
        if (rootTags.isFull()) {
            rootTags = flushRootTags();
        }

        rootTags.registerTag(tagName, hash);
    }

    /**
     * Resolves a tag name into a hash.
     */
    public String resolveTag(String tagName) throws IOException {
        TagBlock currentTags = _header.getRootTags();

        while (currentTags != null) {
            for (int i = 0; i < currentTags.size(); i++) {
                if (currentTags.getEntryName(i).equals(tagName)) {
                    return currentTags.getEntryCommit(i);
                }
            }

            String nextTags = currentTags.getNextPointer();
            if (isEmptyHash(nextTags)) {
                currentTags = null;
            } else {
                currentTags = new TagBlock();
                unserializeBlock(currentTags, nextTags);
            }
        }

        throw new IllegalArgumentException("Could not find definition for tag '" + tagName + "'");
    }

    /**
     * Ensures that a block matching the hash of this block exists in the
     * block pool. Only writes the block if it isn't already stored.
     */
    public String serializeBlock(BasicBlock block) throws IOException {
        ByteBuffer blockBuffer = newBlockBuffer();
        block.serialize(blockBuffer);
        String hash = hashOfLastBlock(blockBuffer);

        long offset = getOffsetForHash(hash.toUpperCase());
        if (offset == HASH_NOT_FOUND) {
            IndexBlock index = _header.getRootIndex();
            if (index.isFull()) {
                index = flushRootIndex();
            }

            blockBuffer.position(0);
            _channel.write(blockBuffer, _channel.size());
            offset = getLastBlockOffset();

            index.registerBlock(hash, offset);
        }

        return hash;
    }

    /**
     * Saves the current root index and initializes a new root index pointing
     * to it.
     *
     * Note that this should only be called if the existing root index is full,
     * otherwise space in the index block will be wasted.
     */
    private IndexBlock flushRootIndex() throws IOException {
        IndexBlock rootIndex = _header.getRootIndex();
        ByteBuffer indexBuffer = newBlockBuffer();
        rootIndex.serialize(indexBuffer);

        String hash = hashOfLastBlock(indexBuffer);
        long offset = getOffsetForHash(hash.toUpperCase());

        IndexBlock newRootIndex = _header.linkInNewIndexBlock(hash);

        if (offset == HASH_NOT_FOUND) {
            indexBuffer.position(0);
            _channel.write(indexBuffer, _channel.size());
            offset = getLastBlockOffset();
            newRootIndex.registerBlock(hash, offset);
        }

        return newRootIndex;
    }

    /**
     * Saves the current root tag block and initializes a new root tag block
     * pointing to it.
     *
     * Note that this should only be called if the existing root tag block is
     * full, otherwise space in the tag block will be wasted.
     */
    private TagBlock flushRootTags() throws IOException {
        TagBlock rootTags = _header.getRootTags();
        String hash = serializeBlock(rootTags);
        TagBlock newRootTags = _header.linkInNewTagBlock(hash);
        return newRootTags;
    }

    /**
     * Loads the data for the given hash and initializes the block with it.
     */
    public void unserializeBlock(BasicBlock block, String hash) throws IOException {
        if (!isValidHash(hash)) {
            throw new IllegalArgumentException(hash + " is not a valid SHA256 hash");
        }

        long offset = getOffsetForHash(hash.toUpperCase());
        if (offset == HASH_IS_EMPTY) {
            throw new IllegalArgumentException("Cannot retrieve block with empty hash");
        } else if (offset == HASH_NOT_FOUND) {
            throw new IllegalArgumentException("Could not find block with hash " + hash);
        } else {
            unserializeBlockAtOffset(block, offset);
        }
    }

    /**
     * Returns the offset of the block most recently written to the store.
     */
    private long getLastBlockOffset() throws IOException {
        return ((_channel.size() - HeaderBlock.HEADER_SIZE_BYTES) / BasicBlock.BLOCK_SIZE_BYTES) - 1;
    }

    /**
     * Initializes the block with the block data at the given offset.
     */
    private void unserializeBlockAtOffset(BasicBlock block, long offset) throws IOException {
        ByteBuffer blockBuffer = newBlockBuffer();
        _channel.read(blockBuffer, HeaderBlock.HEADER_SIZE_BYTES + BasicBlock.BLOCK_SIZE_BYTES * offset);

        blockBuffer.position(0);
        block.unserialize(blockBuffer);
    }

    /**
     * Finds the given hash in the store's index, returning either an offset
     * (in blocks from the end of the header) or the status codes HASH_IS_EMPTY
     *  or HASH_NOT_FOUND.
     */
    private long getOffsetForHash(String hash) throws IOException {
        if (isEmptyHash(hash)) {
            return HASH_IS_EMPTY;
        } else if (_indexCache.containsKey(hash)) {
            return _indexCache.get(hash);
        } else {
            // Although it looks like there's the potential for unlimited
            // recursion here, it's not really a problem because of how index
            // block hashes are ordered.
            //
            // When an index block is filled and flushed into the block store,
            // its hash is written at the top of the next index block in the
            // header. That means that we will always be able to locate the
            // block's offset in the cache.
            IndexBlock currentIndex = _header.getRootIndex();

            while (currentIndex != null) {
                for (int i = 0; i < currentIndex.size(); i++) {
                    String currentHash = currentIndex.getEntryPointer(i);
                    long currentOffset = currentIndex.getEntryOffset(i);
                    _indexCache.put(currentHash, currentOffset);

                    if (currentHash.equals(hash)) {
                        return currentOffset;
                    }
                }

                String nextIndex = currentIndex.getNextPointer();
                long offset = getOffsetForHash(nextIndex);

                if (offset == HASH_IS_EMPTY) {
                    currentIndex = null;
                } else {
                    currentIndex = new IndexBlock();
                    unserializeBlockAtOffset(currentIndex, offset);
                }
            }

            return HASH_NOT_FOUND;
        }
    }
}
