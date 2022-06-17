# CData output plugin for Embulk

Support to CData JDBC Driver bulk upsert or update statements.

## Overview

* **Plugin type**: output
* **Load all or nothing**: no
* **Resume supported**: no
* **Cleanup supported**: yes

## Configuration

- **driver_path**: description (string, required)
- **url**: description (string, `"jdbc:..."`)
- **mode**: description (string, `"insert_direct" | "update"`)
- **table**: description (string, required)
- **external_id_column**: description (string, required)

## Example

```yaml
out:
  type: cdata
  driver_path: lib/awesome.jar
  url: jdbc:...
  mode: merge_direct
  table: TableName
  external_id_column: ExternalIdColumn
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
