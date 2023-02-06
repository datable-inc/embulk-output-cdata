package org.embulk.output.cdata;

import org.embulk.config.TaskReport;
import org.embulk.spi.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.stream.Collectors;

public class CDataPageOutputForInsert implements TransactionalPageOutput {

  private static final Logger logger = LoggerFactory.getLogger(CDataPageOutputForInsert.class);

  private final PageReader pageReader;
  private final Connection conn;
  private final CDataOutputPlugin.PluginTask task;
  private PreparedStatement preparedStatement;
  private String insertTempTable = "";

  public CDataPageOutputForInsert(final PageReader reader, Connection conn, CDataOutputPlugin.PluginTask task) {
    this.pageReader = reader;
    this.conn = conn;
    this.task = task;
    this.preparedStatement = null;
    this.insertTempTable = task.getTable() + "#TEMP";
  }

  @Override
  public void add(Page page) {
    pageReader.setPage(page);

    ArrayList<String> columnNames = pageReader.getSchema().getColumns().stream()
            .map(Column::getName).collect(Collectors.toCollection(ArrayList::new));

    ArrayList<String> preparedValues = pageReader.getSchema().getColumns().stream()
            .map(it -> "?").collect(Collectors.toCollection(ArrayList::new));

    String insertTempStatement = "INSERT INTO `" + insertTempTable + "` (" +
            columnNames.stream().collect(Collectors.joining("`, `", "`", "`")) +
            ") VALUES (" +
            String.join(", ", preparedValues) + ")";
    logger.info(insertTempStatement);

    while (pageReader.nextRecord()) {
      try {
        PreparedStatement preparedStatement = conn.prepareStatement(insertTempStatement, Statement.RETURN_GENERATED_KEYS);

        pageReader.getSchema().visitColumns(createColumnVisitor(preparedStatement));
        preparedStatement.executeUpdate();

        logger.info("inserted to " + insertTempTable);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    String insertStatement = "INSERT INTO `" + task.getTable() + "` (" +
            columnNames.stream().collect(Collectors.joining("`, `", "`", "`")) +
            ") SELECT " +
            columnNames.stream().collect(Collectors.joining("`, `", "`", "`")) +
            " FROM `" + insertTempTable + "`";
    logger.info(insertStatement);

    try {
      this.preparedStatement = conn.prepareStatement(insertStatement, Statement.RETURN_GENERATED_KEYS);
      pageReader.getSchema().visitColumns(createColumnVisitor(preparedStatement));
      preparedStatement.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void finish() {
    try {
      conn.close();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    try {
      conn.close();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void abort() {
    try {
      conn.close();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public TaskReport commit() {
    return null;
  }

  private ColumnVisitor createColumnVisitor(PreparedStatement preparedStatement) {
    return new ColumnVisitor() {
      @Override
      public void booleanColumn(Column column) {
        try {
          if (pageReader.isNull(column)) {
            preparedStatement.setObject(column.getIndex() + 1, null);
          } else {
            preparedStatement.setBoolean(column.getIndex() + 1, pageReader.getBoolean(column));
          }
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void longColumn(Column column) {
        try {
          if (pageReader.isNull(column)) {
            preparedStatement.setObject(column.getIndex() + 1, null);
          } else {
            preparedStatement.setLong(column.getIndex() + 1, pageReader.getLong(column));
          }
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void doubleColumn(Column column) {
        try {
          if (pageReader.isNull(column)) {
            preparedStatement.setObject(column.getIndex() + 1, null);
          } else {
            preparedStatement.setDouble(column.getIndex() + 1, pageReader.getDouble(column));
          }
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void stringColumn(Column column) {
        try {
          if (pageReader.isNull(column)) {
            preparedStatement.setObject(column.getIndex() + 1, null);
          } else {
            preparedStatement.setString(column.getIndex() + 1, pageReader.getString(column));
          }
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void timestampColumn(Column column) {
        try {
          if (pageReader.isNull(column)) {
            preparedStatement.setObject(column.getIndex() + 1, null);
          } else {
            preparedStatement.setTimestamp(column.getIndex() + 1, Timestamp.from(pageReader.getTimestampInstant(column)));
          }
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void jsonColumn(Column column) {
        try {
          if (pageReader.isNull(column)) {
            preparedStatement.setObject(column.getIndex() + 1, null);
          } else {
            preparedStatement.setString(column.getIndex() + 1, pageReader.getString(column));
          }
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }
}
