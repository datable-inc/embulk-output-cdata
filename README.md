# CData output plugin for Embulk

Support to CData JDBC Driver bulk upsert or update statements.

## Overview

* **Plugin type**: output
* **Load all or nothing**: no
* **Resume supported**: no
* **Cleanup supported**: yes

## Configuration

- **driver_path**: description (string, required)
- **class_name**: description (string, required)
- **url**: description (string, `"jdbc:..."`)
- **mode**: description (string, `"upsert" | "update"`)
- **table**: description (string, required)
- **external_id_column**: description (string, required)
- **default_primary_key**: description (string, optional, default: "id")

## Example

```yaml
out:
  type: cdata
  driver_path: lib/awesome.jar
  class_name: yourAwesomeClassName
  url: jdbc:...
  mode: upsert
  table: TableName
  external_id_column: ExternalIdColumn
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
