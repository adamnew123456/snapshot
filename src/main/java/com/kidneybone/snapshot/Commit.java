package com.kidneybone.snapshot;

import static com.kidneybone.snapshot.blocks.BlockUtils.isEmptyHash;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Stack;

import com.kidneybone.snapshot.blocks.BasicBlock;
import com.kidneybone.snapshot.blocks.CommitDataBlock;
import com.kidneybone.snapshot.blocks.DataBlock;
import com.kidneybone.snapshot.blocks.FileBlock;

class Commit {
    private BlockStore _store;

    class FileInfo {
        public final String fileBlock;
        public final long size;

        public FileInfo(String fileBlock, long size) {
            this.fileBlock = fileBlock;
            this.size = size;
        }
    }

    public Commit(BlockStore store) {
        _store = store;
    }

    /**
     * Builds a new commit data block representing the current state of the
     * directory (or possibly multiple), stores them, and returns the
     * hash of the last.
     */
    public String storeFileTree(String baseDirectory) throws IOException {
        if (baseDirectory.endsWith(File.separator)) {
            baseDirectory = baseDirectory.substring(0, baseDirectory.length() - 1);
        }

        Path basePath = Path.of(baseDirectory);

        Stack<String> directoriesToScan = new Stack<String>();
        directoriesToScan.push(baseDirectory);

        CommitDataBlock dataBlock = new CommitDataBlock();

        while (!directoriesToScan.empty()) {
            String searchDirectory = directoriesToScan.pop();
            String[] entries = new File(searchDirectory).list();

            for (String entry: entries) {
                String fullPath = searchDirectory + File.separator + entry;
                File entryInfo = new File(fullPath);

                if (entryInfo.isDirectory()) {
                    directoriesToScan.push(fullPath);
                } else {
                    if (dataBlock.isFull()) {
                        String dataHash = _store.serializeBlock(dataBlock);
                        dataBlock = new CommitDataBlock(dataHash);
                    }

                    Path path = FileSystems.getDefault().getPath(fullPath);
                    FileInfo fileBlock = storeFileBlock(path);
                    dataBlock.registerFile(basePath.relativize(path).toString().replace("\\", "/"),
                                           fileBlock.size,
                                           fileBlock.fileBlock);
                }
            }
        }

        return _store.serializeBlock(dataBlock);
    }

    /**
     * Reads the data from the data block (and its parents), and restores the
     * data referenced by it into the given directory.
     */
    public void restoreFileTree(String baseDirectory, String dataPointer) throws IOException {
        CommitDataBlock dataBlock = new CommitDataBlock();
        _store.unserializeBlock(dataBlock, dataPointer);

        while (dataBlock != null) {
            for (int i = 0; i < dataBlock.size(); i++) {
                Path path  = Path.of(baseDirectory, dataBlock.getEntryPath(i));
                long size = dataBlock.getEntrySize(i);
                String filePointer = dataBlock.getEntryFilePointer(i);

                new File(path.getParent().toString()).mkdirs();
                restoreFileBlock(path, filePointer, size);
            }

            String nextBlock = dataBlock.getNextPointer();
            if (isEmptyHash(nextBlock)) {
                dataBlock = null;
            } else {
                dataBlock = new CommitDataBlock();
                _store.unserializeBlock(dataBlock, nextBlock);
            }
        }
    }

    /**
     * Reads the given file, producing and storing a series of data blocks, as
     * well as a file block containing all of them. Returns a structure containing
     * both the hash of the file block as well as the size of the file itself.
     */
    private FileInfo storeFileBlock(Path filePath) throws IOException {
        FileChannel fileData = null;
        FileBlock currentFileBlock = new FileBlock();
        long size;

        try {
            fileData = FileChannel.open(filePath, StandardOpenOption.READ);
            size = fileData.size();
            ByteBuffer dataBuffer = ByteBuffer.allocate(BasicBlock.BLOCK_SIZE_BYTES);

            int blockSize = fileData.read(dataBuffer);
            while (blockSize > 0) {
                dataBuffer.position(0);
                DataBlock data = new DataBlock(dataBuffer);
                String dataHash = _store.serializeBlock(data);

                if (currentFileBlock.isFull()) {
                    String fileHash = _store.serializeBlock(currentFileBlock);
                    currentFileBlock = new FileBlock(fileHash);
                }

                currentFileBlock.registerBlock(dataHash);

                dataBuffer.position(0);
                blockSize = fileData.read(dataBuffer);
            }
        } finally {
            if (fileData != null) fileData.close();
        }

        return new FileInfo(_store.serializeBlock(currentFileBlock), size);
    }

    /**
     * Reads the given file, producing and storing a series of data blocks, as
     * well as a file block containing all of them. Returns a structure containing
     * both the hash of the file block as well as the size of the file itself.
     */
    private void restoreFileBlock(Path filePath, String filePointer, long size) throws IOException {
        FileChannel fileData = null;
        FileBlock currentFileBlock = new FileBlock();
        _store.unserializeBlock(currentFileBlock, filePointer);

        try {
            fileData = FileChannel.open(filePath, StandardOpenOption.WRITE, StandardOpenOption.CREATE);

            ArrayList<String> dataBlocks = new ArrayList<String>();
            while (currentFileBlock != null) {
                // While the linked data block structure is reversed as a whole, the
                // individual data blocks within the structure all contain data blocks
                // in the original order. That means that we have to iterate each block
                // in reverse in order to get all of dataBlocks in reverse.
                for (int i = currentFileBlock.size() - 1; i >= 0; i--) {
                    dataBlocks.add(currentFileBlock.getEntryPointer(i));
                }

                String nextBlock = currentFileBlock.getPreviousPointer();
                if (isEmptyHash(nextBlock)) {
                    currentFileBlock = null;
                } else {
                    currentFileBlock = new FileBlock();
                    _store.unserializeBlock(currentFileBlock, nextBlock);
                }
            }

            Collections.reverse(dataBlocks);

            DataBlock currentDataBlock = new DataBlock();
            ByteBuffer dataBuffer = ByteBuffer.allocate(BasicBlock.BLOCK_SIZE_BYTES);
            for (String dataPointer: dataBlocks) {
                _store.unserializeBlock(currentDataBlock, dataPointer);
                dataBuffer.position(0);
                size -= currentDataBlock.writeContent(dataBuffer, size);
                dataBuffer.flip();

                fileData.write(dataBuffer);
                dataBuffer.clear();
            }

            fileData.force(true);
        } finally {
            if (fileData != null) fileData.close();
        }
    }
}
