# LEngine

LEngine is under development.

## Goal

* Performance

## Non-goals

* Fault tolerance
* Database (not even KVDB)

## Features

* Atomic updates

### Future?

* Garbage collection
* User-defined operations

## Wire Format

### Log file

The log file is append-only.

```
+-------+-------+-----+----------+-------+-----+----------+
| Entry | Entry | ... | Sentinel | Entry | ... | Sentinel |
+-------+-------+-----+----------+-------+-----+----------+
```

* An entry is a region of contiguous bytes. Its size is undetermined.
* A sentinel is a special entry. (Currently, they're just entries.)

All data are stored as-is.

### Index file

The index file is append-only.

```
+--------------------+
| RowId1 <-> Offset1 |
+--------------------+
| RowId2 <-> Offset2 |
+--------------------+
| RowId3 <-> Offset3 |
+--------------------+
|        ...         |
```

The row ID is assumed to grow contiguously, so it's not stored in the
file.
