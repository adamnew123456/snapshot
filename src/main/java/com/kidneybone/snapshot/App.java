package com.kidneybone.snapshot;

import static com.kidneybone.snapshot.blocks.BlockUtils.isEmptyHash;

import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import com.kidneybone.snapshot.blocks.CommitBlock;

public class App {
    private static SimpleDateFormat _dateFormatter =
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z", Locale.getDefault());

    private static void displayLog(BlockStore store) throws Exception {
        String nextCommitHash = store.getHeader().getLastCommit();
        CommitBlock commit = new CommitBlock();

        while (!isEmptyHash(nextCommitHash)) {
            store.unserializeBlock(commit, nextCommitHash);
            Date commitDate = new Date(commit.getTimestamp());
            System.out.printf("Commit %s @ %s\n", nextCommitHash, _dateFormatter.format(commitDate));

            nextCommitHash = commit.getPreviousCommit();
            commit = new CommitBlock();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println(
                "snapshot commit SNAPSHOT-FILE DIRECTORY\n" +
                "snapshot tag SNAPSHOT-FILE ADDRESS NAME\n" +
                "snapshot log SNAPSHOT-FILE\n" +
                "snapshot restore SNAPSHOT-FILE DIRECTORY (-t TAG-NAME | -a ADDRESS)\n" +
                "snapshot viz SNAPSHOT-FILE");
            System.exit(1);
        }

        String command = args[0];
        Path snapshotFile = Path.of(args[1]);

        FileChannel channel = FileChannel.open(snapshotFile, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        BlockStore store = new BlockStore(channel);

        if (channel.size() == 0) {
            // Make sure that the file has a valid header, even if it was previously empty
            store.serialize();
        }

        store.unserialize();

        if (command.equals("commit")) {
            if (args.length != 3) {
                System.err.println("Invalid number of arguments: snapshot commit SNAPSHOT-FILE DIRECTORY");
                System.exit(1);
            }

            store.addCommit(args[2]);
            store.serialize();

        } else if (command.equals("tag")) {
            if (args.length != 4) {
                System.err.println("Invalid number of arguments: snapshot tag SNAPSHOT-FILE ADDRESS NAME");
                System.exit(1);
            }

            store.addTag(args[3], args[2]);
            store.serialize();

        } else if (command.equals("log")) {
            if (args.length != 2) {
                System.err.println("Invalid number of arguments: snapshot log SNAPSHOT-FILE");
                System.exit(1);
            }

            displayLog(store);

        } else if (command.equals("restore")) {
            if (args.length != 5) {
                System.err.println("Invalid number of arguments: snapshot restore SNAPSHOT-FILE DIRECTORY (-t TAG-NAME | -a ADDRESS)");
                System.exit(1);
            }

            String hash = null;
            if (args[3].equals("-t")) {
                hash = store.resolveTag(args[4]);
            } else if (args[3].equals("-a")) {
                hash = args[4];
            } else {
                System.err.println("-a or -t required: snapshot restore SNAPSHOT-FILE DIRECTORY (-t TAG-NAME | -a ADDRESS)");
                System.exit(1);
            }

            store.restoreCommit(args[2], hash);

        } else if (command.equals("viz")) {
            if (args.length != 2) {
                System.err.println("Invalid number of arguments: snapshot viz SNAPSHOT-FILE");
                System.exit(1);
            }

            Visualizer viz = new Visualizer(store, _dateFormatter);
            viz.visualizeStore();

        } else {
            System.err.println("Command not recognized: " + command);
            System.exit(1);
        }

        channel.close();
    }
}
