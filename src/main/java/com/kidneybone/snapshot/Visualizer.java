package com.kidneybone.snapshot;

import static com.kidneybone.snapshot.blocks.BlockUtils.hashBlock;
import static com.kidneybone.snapshot.blocks.BlockUtils.isEmptyHash;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.kidneybone.snapshot.blocks.CommitBlock;
import com.kidneybone.snapshot.blocks.CommitDataBlock;
import com.kidneybone.snapshot.blocks.FileBlock;
import com.kidneybone.snapshot.blocks.IndexBlock;
import com.kidneybone.snapshot.blocks.TagBlock;

public class Visualizer {
    private BlockStore _store;
    private SimpleDateFormat _dateFormatter;

    public Visualizer(BlockStore store, SimpleDateFormat dateFormatter) {
        _store = store;
        _dateFormatter = dateFormatter;
    }

    private String escapeGraphLabel(String s) {
        return s.replace("\"", "\\\"")
            .replace("\\", "\\\\")
            .replace("|", "\\|")
            .replace("<", "\\<")
            .replace(">", "\\>")
            .replace("{", "\\{")
            .replace("}", "\\}");
    }

    private String escapeHash(String hash) {
        return escapeGraphLabel(hash.substring(0, 6));
    }

    private void visualizeFile(FileBlock file) throws Exception {
        String fileHash = hashBlock(file);
        StringBuilder blockLabel = new StringBuilder();
        blockLabel.append("Kind: file | Hash: ").append(escapeHash(fileHash));
        blockLabel.append(" | Size: ").append(file.size());
        for (int i = 0; i < file.size(); i++) {
            String node = "<data" + i + ">";
            blockLabel.append("| ").append(node).append("Data: ");
            blockLabel.append(escapeHash(file.getEntryPointer(i)));
        }

        blockLabel.append(" | <prev> Previous: ");
        blockLabel.append(escapeHash(file.getPreviousPointer()));

        System.out.printf("\"%s\" [label=\"%s\"];\n", fileHash, blockLabel.toString());

        for (int i = 0; i < file.size(); i++) {
            System.out.printf("\"%s\":data%d -> \"%s\";\n",
                              escapeGraphLabel(fileHash),
                              i,
                              escapeGraphLabel(file.getEntryPointer(i)));
        }

        System.out.printf("\"%s\":prev -> \"%s\";\n",
                          escapeGraphLabel(fileHash),
                          escapeGraphLabel(file.getPreviousPointer()));
    }

    private void visualizeCommitData(CommitDataBlock commitData) throws Exception {
        String dataHash = hashBlock(commitData);
        StringBuilder blockLabel = new StringBuilder();
        blockLabel.append("Kind: commitdata | Hash: ").append(escapeHash(dataHash));
        blockLabel.append(" | Size: ").append(commitData.size());

        for (int i = 0; i < commitData.size(); i++) {
            blockLabel.append("| { Path: ");
            blockLabel.append(escapeGraphLabel(commitData.getEntryPath(i)));
            blockLabel.append(" | Size: ");
            blockLabel.append(commitData.getEntrySize(i));

            String node = "<file" + i + ">";
            blockLabel.append(" | ").append(node).append(" File: ");
            blockLabel.append(escapeHash(commitData.getEntryFilePointer(i)));
            blockLabel.append("}");
        }

        blockLabel.append(" | <next> Next: ");
        blockLabel.append(escapeHash(commitData.getNextPointer()));

        System.out.printf("\"%s\" [label=\"%s\"];\n",
                          escapeGraphLabel(dataHash),
                          blockLabel.toString());

        for (int i = 0; i < commitData.size(); i++) {
            FileBlock file = new FileBlock();
            String lastFileHash = commitData.getEntryFilePointer(i);

            while (!isEmptyHash(lastFileHash)) {
                _store.unserializeBlock(file, commitData.getEntryFilePointer(i));
                visualizeFile(file);

                lastFileHash = file.getPreviousPointer();
                file = new FileBlock();
            }

            System.out.printf("\"%s\":\"file%d\"\n -> \"%s\";\n",
                              escapeGraphLabel(dataHash),
                              i,
                              escapeGraphLabel(commitData.getEntryFilePointer(i)));
        }

        System.out.printf("\"%s\":next -> \"%s\"\n",
                          escapeGraphLabel(dataHash),
                          escapeGraphLabel(commitData.getNextPointer()));
    }

    private void visualizeCommit(CommitBlock commit) throws Exception {
        String commitHash = hashBlock(commit);
        Date commitDate = new Date(commit.getTimestamp());

        System.out.printf("\"%s\" [label=\"Kind: commit | Hash: %s | Timestamp: %s | <data> Data: %s | <prev> Parent: %s \"];\n",
                          commitHash,
                          escapeHash(commitHash),
                          escapeGraphLabel(_dateFormatter.format(commitDate)),
                          escapeHash(commit.getDataPointer()),
                          escapeHash(commit.getPreviousCommit()));

        CommitDataBlock commitData = new CommitDataBlock();
        String lastCommitDataHash = commit.getDataPointer();
        while (!isEmptyHash(lastCommitDataHash)) {
            _store.unserializeBlock(commitData, lastCommitDataHash);
            visualizeCommitData(commitData);

            lastCommitDataHash = commitData.getNextPointer();
            commitData = new CommitDataBlock();
        }

        System.out.printf("\"%s\":prev -> \"%s\";\n",
                          escapeGraphLabel(commitHash),
                          escapeGraphLabel(commit.getPreviousCommit()));
        System.out.printf("\"%s\":data -> \"%s\";\n",
                          escapeGraphLabel(commitHash),
                          escapeGraphLabel(commit.getDataPointer()));
    }

    private void visualizeTags(TagBlock tags) throws Exception {
        String tagHash = hashBlock(tags);
        StringBuilder blockLabel = new StringBuilder();
        blockLabel.append("Kind: tags | Hash: ").append(escapeHash(tagHash));
        blockLabel.append(" | Size: ").append(tags.size());
        for (int i = 0; i < tags.size(); i++) {
            String node = "<tag" + i + ">";
            blockLabel.append("| {").append(node).append("Name: ");
            blockLabel.append(escapeGraphLabel(tags.getEntryName(i)));
            blockLabel.append(" | ").append(node).append("Commit: ");
            blockLabel.append(escapeHash(tags.getEntryCommit(i)));
            blockLabel.append("}");
        }

        blockLabel.append(" | <next> Next: ");
        blockLabel.append(escapeHash(tags.getNextPointer()));

        System.out.printf("\"%s\" [label=\"%s\"];\n", tagHash, blockLabel.toString());

        for (int i = 0; i < tags.size(); i++) {
            System.out.printf("\"%s\":\"tag%d\" -> \"%s\";\n",
                              escapeGraphLabel(tagHash),
                              i,
                              escapeGraphLabel(tags.getEntryCommit(i)));
        }

        System.out.printf("\"%s\":next -> \"%s\"\n",
                          escapeGraphLabel(tagHash),
                          escapeGraphLabel(tags.getNextPointer()));
    }

    private void visualizeIndex(IndexBlock index) throws Exception {
        String indexHash = hashBlock(index);
        StringBuilder blockLabel = new StringBuilder();
        blockLabel.append("Kind: index | Hash: ").append(escapeHash(indexHash));
        blockLabel.append(" | Size: ").append(index.size());
        for (int i = 0; i < index.size(); i++) {
            String node = "<index" + i + ">";
            blockLabel.append("| {").append(node).append(" ");
            blockLabel.append(escapeHash(index.getEntryPointer(i)));
            blockLabel.append(" ").append("@ ");
            blockLabel.append(index.getEntryOffset(i));
            if (index.getEntryIsCompressed(i)) {
                blockLabel.append(" (zip)");
            }
            blockLabel.append("}");
        }

        blockLabel.append(" | <next> Next: ");
        blockLabel.append(escapeHash(index.getNextPointer()));

        System.out.printf("\"%s\" [label=\"%s\"];\n", indexHash, blockLabel.toString());

        for (int i = 0; i < index.size(); i++) {
            System.out.printf("\"%s\":\"index%d\" -> \"%s\";\n",
                              escapeGraphLabel(indexHash),
                              i,
                              escapeGraphLabel(index.getEntryPointer(i)));
        }

        System.out.printf("\"%s\":next -> \"%s\"\n",
                          escapeGraphLabel(indexHash),
                          escapeGraphLabel(index.getNextPointer()));
    }

    public void visualizeStore() throws Exception {
        System.out.println("digraph snapshot {");
        System.out.println("rankdir=LR");
        System.out.println("node [shape=record];");

        String nextCommitHash = _store.getHeader().getLastCommit();
        CommitBlock commit = new CommitBlock();

        while (!isEmptyHash(nextCommitHash)) {
            _store.unserializeBlock(commit, nextCommitHash);
            visualizeCommit(commit);

            nextCommitHash = commit.getPreviousCommit();
            commit = new CommitBlock();
        }

        TagBlock tags = _store.getHeader().getRootTags();
        while (tags != null) {
            visualizeTags(tags);

            String nextTags = tags.getNextPointer();
            if (isEmptyHash(nextTags)) {
                tags = null;
            } else {
                tags = new TagBlock();
                _store.unserializeBlock(tags, nextTags);
            }
        }

        IndexBlock index = _store.getHeader().getRootIndex();
        while (index != null) {
            visualizeIndex(index);

            String nextIndex = index.getNextPointer();
            if (isEmptyHash(nextIndex)) {
                index = null;
            } else {
                index = new IndexBlock();
                _store.unserializeBlock(index, nextIndex);
            }
        }

        System.out.println("}");
    }
}
