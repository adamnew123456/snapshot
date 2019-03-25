# snapshot

**THIS TOO IS ONLY A PROOF OF CONCEPT! DO NOT STORE ANYTHING IMPORTANT IN IT!**

## What is this?

Snapshot is a file archival tool which lets you store multiple versions of a
directory tree in a single file. You can think of it as a lobotomized version
control system which can only commit and restore individual snapshots.

The basic use would be something like this, where you have a directory whose
contents you want to track over time.

```shell
$ # Store the current contents of my-work-directory into the snapshot
$ snapshot commit snapshot-file.ss my-work-directory/

$ # Make some changes in the work directory
$ vim my-work-directory/...

$ # Update the snapshot to include the new contents.
$ snapshot commit snapshot-file.ss my-work-directory/

$ # Display the commits
$ snapshot log snapshot-file.ss

$ # Mark a commit using a human-readable name
$ snapshot tag snapshot-file.ss B283FD... mondays-work

$ # Restore an older snapshot into its own directory
$ mkdir old-work-directory
$ snapshot restore snapshot-file.ss old-work-directory -t mondays-work
```

### How can I build it?

Via Maven:

```shell
$ mvn clean compile jar:jar
```

## How does it work?

### Overview

Internally, a snapshot file is organized like a basic filesystem - it contains a
simple header, as well as a group of blocks which contain metadata (commits,
indexes, etc.) as well as the contents of the files in each commit.

Specifically, a snapshot is a *content-addressible* filesystem, where each
entity in the snapshot is identified by its contents. Like in Git, everything is
identified by a SHA-256 hash, which the output of the log command makes clear:

```text
Commit B0A0606000E0009010D0506060405050B09090B0A0A00090A090201040708070 @ 2019-03-24 16:40:21 EDT
Commit A02050F0C070F0F0D020A030E0F080C0F0E0805030302060E010C0004040C040 @ 2019-03-24 16:39:45 EDT
```

One big difference with Git is that snapshots don't rely on a host filesystem to
determine where things are stored. Instead of taking that hash and mapping it to
a directory stored on an ext4 or NTFS volume, snapshots have their own internal
indexes which let them map content addresses to offsets.

This also means that snapshots are naturally deduplicated - since a block's
address is just its contents, it's easy to avoid writing the same information in
multiple places.

Snapshots are also append-only, which makes them great for preservation,
although it does force a few compromises in the design that will be covered
later.

Also unlike Git and like most filesystems, a snapshot is structured as a series
of fixed-sized (64 KB) blocks. Any entity bigger than a block will be split up
among multiple blocks, which is why almost every kind of block has a "next
pointer".

(One note about these next pointers - they're the only place where the "empty
hash" consisting of all zeroes is allowed. You can think of it as the NULL
pointer at the end of a linked list)


If you want to get an overview of the structure of a snapshot, you can use the
`viz` subcommand, which will generate a dot file that you can render using the
standard GraphViz tools:

```shell
$ snapshot viz snapshot-file.ss > snapshot.dot
$ dot -Tsvg snapshot.dot > snapshot.svg
```

A snapshot file makes use of 6 kinds of blocks, not including the header. Since
a header is based upon the other types of blocks, it's included at the end of
this list.

### Data Blocks

Data blocks are the simplest kind of block, which contain 64 KB of arbitrary
data. When you make a commit, the contents of the files in your working
directory are stored into these blocks.

If an entire block isn't used, then it's padded out to 64 KB for zeroes. The
snapshot file keeps track of the size elsewhere, so if the file is less than
64 KB only the relevant part is extracted.

However, if a file is greater than 64 KB (or, in snapshot's case, even if it
isn't), then there needs to be a way to connect multiple data blocks into a
whole file. That's what file blocks are for.

### File Blocks

```text
| data block addresses   [0]  |
| data block addresses   [1]  |
| ...                         |
| data block addresses[1022]  |
| previous file block address |
```

A file block is just a list of addresses to data blocks, and a link to the file
block immediately before this one. It doesn't contain a name or a size, which
allows it to be re-used in places where that information isn't the same (for
example, if you move a file to a different path or add some data onto the end of
it).

Also, it's important to note that the pointer is to the previous file block and
not the next one. Internally each block goes from first data block to the last,
but the blocks themselves are ordered from last to first:

```text
+-------|-------|-------+
| G H I | D E F | A B C |
+-------|-------|-------+
```

### Commit Data Blocks

```text
| file path (4KB)     [0] |
| file size (64-bit)  [0] |
| file block pointer  [0] |
| ...                     |
| file path          [14] |
| file size          [14] |
| file block pointer [14] |
| next commit data addr. |
```

Commit data blocks contain references to all the files included in a commit.

One important note is that the path (like all user-provided strings) are encoded
as UTF-8, and are zero padded to fill up the field containing them. They're also
relative to the directory the snapshot is of, so that you have a tree like this:

- a/
  - b/
    - c
  - d
  - e
  
And take a snapshot of a/, then the paths in the snapshot will be "b/c", "d" and
"e".

Also, like Git, directories are not tracked as separate entities. Snapshot knows
to create them when restoring a snapshot, but otherwise they exist only in paths.

### Commit Blocks

```text
| ms since Unix epoch (64-bit) |
| commit data address          |
| previous commit address      |
```

Commits contain metadata about a commit, but mostly serve as a place to store
addresses that provide both the file contents of the commit, and the history
in the form of the previous commit.

One thing worth noting is that commit blocks waste a lot of space, since they
use a small fraction of the entire 64 KB of the block. It would be possible to
pack multiple commits like we've seen with the other blocks so far, but that
would make addressing a commit more complicated (since it would be necessary
to know both the block the commit is in and its index in the block).

### Tag Blocks

```text
| tag name (64 bytes) [0] |
| address             [0] |
| ...                     |
| tag name          [510] |
| address           [510] |
| next tag address        |
```

Tags are fairly straightforward, in that they're just alternative names for
addresses. Since a snapshot doesn't have a built-in idea of what a name is
(unlike a hierarchical filesystem), tag blocks are necessary to map those
names into snapshot native addresses.

Although this is an index of a kind, it isn't the central kind of index used in
a snapshot, which maps addresses into concrete offsets in the snapshot file.
That mapping is managed by the index file.

### Index Blocks

```text
| address              [0] |
| offset in snapshot   [0] |
| ...                      |
| address            [510] |
| offset in snapshot [510] |
| next index address       |
```

Index blocks are what ultimately make snapshots content-addressible: any time a
block has to be loaded from the snapshot by its hash, the index is scanned for
that hash. The offset associated with that address is where the data for that
block is ultimately loaded from.

(A minor point is that the offsets here are in terms of block offsets from the
header, which means that offset 0 will be over 130 KB into the file, offset 1
will be an additional 64 KB into the file, and so on)

There are two big questions that come to mind with this structure:

- *How can I find the address of the next index block?* The solution here is
  similar to how DNS solves this problem via glue records: when an index block
  becomes full and needs to be split, the address/offset mapping is stored into
  the newest index. Since indexes grow backwards (like commits), that means that
  the address of an index block will always be available before it needs to be
  loaded.
  
- *How can I find the first index block?* The first index block is always a part
  of the header, to avoid this problem (among others).

### The Header Block

```text
| address of most recent commit |
| root index block (inline)     |
| root tag block (inline)       |
```

As mentioned in the index block section, there has to be a way to determine
where the first object of each kind is, so that the links it has to other
objects can be traversed. 

This is especially important for the index block, which is part of why it's
included inline instead of referenced via an address - if all you have is the
address of the root index block, the only way to resolve that address is to look
at the root index block!

The other big reason to include these blocks inline is that they allow for
efficient use of space within tags and indexes. Unlike commit data and data
blocks, the index and tags aren't built up over a single invocation of the
snapshot program - they have to be open to modification over multiple runs.
Since snapshot blocks are append-only (old data cannot be removed), the only way
to keep them open is to store them outside of the main block pool. When they get
filled, they can be flushed to the block pool since they can't fit any more
data.

Commits are the one exception, since (as mentioned before) commits use an
inefficient storage mechanism by design so that they can be easily addressed.
That's why its useful to store only the address of the most recent commit,
instead of keeping the most recent block of them in the header.
