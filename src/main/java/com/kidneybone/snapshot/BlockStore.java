package com.kidneybone.snapshot;

import static com.kidneybone.snapshot.blocks.BlockUtils.hashOfLastBlock;
import static com.kidneybone.snapshot.blocks.BlockUtils.isEmptyHash;
import static com.kidneybone.snapshot.blocks.BlockUtils.isValidHash;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Date;
import java.util.HashMap;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import com.kidneybone.snapshot.blocks.BasicBlock;
import com.kidneybone.snapshot.blocks.CommitBlock;
import com.kidneybone.snapshot.blocks.IndexBlock;
import com.kidneybone.snapshot.blocks.TagBlock;

class BlockLayout {
    public final static BlockLayout NOT_FOUND = new BlockLayout("NOT-FOUND", 0, 0, false);
    public final static BlockLayout EMPTY_HASH = new BlockLayout("EMPTY-HASH", 0, 0, false);

    public final String hash;
    public final int size;
    public final long offset;
    public final boolean isCompressed;

    public BlockLayout(String hash, long offset, int size, boolean isCompressed) {
        this.hash = hash;
        this.offset = offset;
        this.size = size;
        this.isCompressed = isCompressed;
    }
}

public class BlockStore {
    private FileChannel _channel;
    private HeaderBlock _header = new HeaderBlock();
    private HashMap<String, BlockLayout> _indexCache = new HashMap<>();

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

        headerBuffer.flip();
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

        headerBuffer.flip();
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

        BlockLayout info = getBlockLayoutForHash(hash.toUpperCase());
        if (info == BlockLayout.EMPTY_HASH || info == BlockLayout.NOT_FOUND) {
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
        return serializeBlockInternal(block, true).hash;
    }

    public BlockLayout serializeBlockInternal(BasicBlock block, boolean writeIndex) throws IOException {
        ByteBuffer blockBuffer = newBlockBuffer();
        block.serialize(blockBuffer);
        String hash = hashOfLastBlock(blockBuffer);
        if (hash.equals(BasicBlock.EMPTY_HASH)) {
            throw new IOException("Cannot serialize block whose hash is the empty hash");
        }

        BlockLayout info = getBlockLayoutForHash(hash.toUpperCase());
        int writeSize = 0;
        long offset = 0;
        boolean isCompressed = false;

        if (info == BlockLayout.NOT_FOUND) {
            IndexBlock index = _header.getRootIndex();
            if (writeIndex && index.isFull()) {
                index = flushRootIndex();
            }

            offset = getCurrenttBlockOffset();
            int deflatedSize = 0;

            Deflater deflater = new Deflater();
            deflater.setInput(blockBuffer);
            deflater.finish();

            ByteBuffer deflateBuffer = newBlockBuffer();
            deflatedSize = deflater.deflate(deflateBuffer);

            // We should only take the uncompressed form if the deflater
            // couldn't fit the compressed form within a single block. This
            // usually happens if we're storing something like a JPEG which is
            // already compressed.
            if (deflater.finished()) {
                deflateBuffer.flip();
                _channel.write(deflateBuffer, _channel.size());

                writeSize = deflatedSize;
                isCompressed = true;
            } else {
                blockBuffer.rewind();
                _channel.write(blockBuffer, _channel.size());

                writeSize = BasicBlock.BLOCK_SIZE_BYTES;
                isCompressed = false;
            }

            if (writeIndex) {
                index.registerBlock(hash, offset, writeSize, isCompressed);
            }
        }

        return new BlockLayout(hash, offset, writeSize, isCompressed);
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
        BlockLayout layout = serializeBlockInternal(rootIndex, false);
        IndexBlock newRootIndex;

        // If the hash was already written, we still have to copy it into the
        // new index so that it can be resolved
        if (layout.size == 0) {
            layout = getBlockLayoutForHash(hash);
        }

        newRootIndex = _header.linkInNewIndexBlock(hash);
        newRootIndex.registerBlock(hash, layout.offset, layout.size, layout.isCompressed);
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

        BlockLayout info = getBlockLayoutForHash(hash.toUpperCase());
        if (info == BlockLayout.EMPTY_HASH) {
            throw new IllegalArgumentException("Cannot retrieve block with empty hash");
        } else if (info == BlockLayout.NOT_FOUND) {
            throw new IllegalArgumentException("Could not find block with hash " + hash);
        } else {
                unserializeBlockAtOffset(block, info.offset, info.size, info.isCompressed);
        }
    }

    /**
     * Returns the offset of the block most recently written to the store.
     */
    private long getCurrenttBlockOffset() throws IOException {
        return _channel.size();
    }

    /**
     * Initializes the block with the block data at the given offset.
     */
        private void unserializeBlockAtOffset(BasicBlock block, long offset, int size, boolean compressed) throws IOException {
        ByteBuffer blockBuffer = newBlockBuffer();

        if (compressed) {
            ByteBuffer inflateBuffer = ByteBuffer.allocate(size);
            _channel.read(inflateBuffer, offset);

            inflateBuffer.flip();
            Inflater inflater = new Inflater();
            inflater.setInput(inflateBuffer);

            try {
                int blockSize = inflater.inflate(blockBuffer);
                inflater.end();

                if (blockSize != BasicBlock.BLOCK_SIZE_BYTES) {
                    String error = String.format("Found %s block with size %d after decompression, should be %d",
                                                block.getClass().getName(),
                                                blockSize,
                                                BasicBlock.BLOCK_SIZE_BYTES);
                    throw new IOException(error);
                }
            } catch (DataFormatException err) {
                throw new IOException("Failure when decoding compressed block: " + err.getMessage());
            }
        } else {
            _channel.read(blockBuffer, offset);
        }

        blockBuffer.flip();
        block.unserialize(blockBuffer);
    }

    /**
     * Finds the given hash in the store's index, returning either an offset
     * (in blocks from the end of the header) or the status codes HASH_IS_EMPTY
     *  or HASH_NOT_FOUND.
     */
    private BlockLayout getBlockLayoutForHash(String hash) throws IOException {
        if (isEmptyHash(hash)) {
            return BlockLayout.EMPTY_HASH;
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
                    int currentSize = currentIndex.getEntrySize(i);
                    boolean currentIsCompressed = currentIndex.getEntryIsCompressed(i);
                    BlockLayout info = new BlockLayout(hash, currentOffset, currentSize, currentIsCompressed);
                    _indexCache.put(currentHash, info);

                    if (currentHash.equals(hash)) {
                        return info;
                    }
                }

                String nextIndex = currentIndex.getNextPointer();
                BlockLayout indexInfo = getBlockLayoutForHash(nextIndex);

                if (indexInfo == BlockLayout.EMPTY_HASH) {
                    currentIndex = null;
                } else {
                    currentIndex = new IndexBlock();
                    unserializeBlockAtOffset(currentIndex, indexInfo.offset, indexInfo.size, indexInfo.isCompressed);
                }
            }

            return BlockLayout.NOT_FOUND;
        }
    }
}
