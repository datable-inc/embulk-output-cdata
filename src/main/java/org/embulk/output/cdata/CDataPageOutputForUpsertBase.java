package org.embulk.output.cdata;

import org.embulk.config.TaskReport;
import org.embulk.spi.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;
import java.util.stream.Collectors;

public class CDataPageOutputForUpsertBase implements TransactionalPageOutput {

  private static final Logger logger = LoggerFactory.getLogger(CDataPageOutputForUpsertBase.class);

  private final PageReader pageReader;
  private final Connection conn;
  private final CDataOutputPlugin.PluginTask task;

  public CDataPageOutputForUpsertBase(final PageReader reader, Connection conn, CDataOutputPlugin.PluginTask task) {
    this.pageReader = reader;
    this.conn = conn;
    this.task = task;
  }

  @Override
  public void add(Page page) {
    pageReader.setPage(page);
    List<String> columnNames = createColumns();
    List<String> preparedValues = createPlaceHolders(); // for ExternalIdColumn
    ExecutedInsertResult result;

    try {
      result = executeInsert(columnNames, preparedValues);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    try {
      String upsertStatement = executeUpsert(task.getTable(), columnNames, result);
      logger.info(upsertStatement);
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

  protected PageReader getPageReader() {
    return this.pageReader;
  }

  protected Connection getConnection() {
    return this.conn;
  }

  protected CDataOutputPlugin.PluginTask getTask() {
    return this.task;
  }

  protected List<String> createColumns() {
    throw new UnsupportedOperationException("createColumns is not implemented");
  }

  protected List<String> createPlaceHolders() {
    throw new UnsupportedOperationException("createPlaceHolders is not implemented");
  }

  /**
   * Create placeholder insert query
   * @param tableName
   * @param columnNames
   * @param preparedValues
   * @return
   */
  protected String createInsertQuery(String tableName, List<String> columnNames, List<String> preparedValues) {
    return "INSERT INTO `" + tableName + "` (" +
            columnNames.stream().collect(Collectors.joining("`, `", "`", "`")) +
            ") VALUES (" +
            String.join(", ", preparedValues) + ")";
  }

  /**
   * insert into Temp table
   * @param columnNames
   * @param preparedValues
   * @return ExecutedInsertResult
   * @throws SQLException
   */
  protected ExecutedInsertResult executeInsert(List<String> columnNames, List<String> preparedValues) throws SQLException {
    throw new UnsupportedOperationException("executeInsert is not implemented");
  }

  /**
   * execute upsert query
   * when not upsert query un suppoerted, must be use "INSERT INTO SELECT" and "UPDATE SELECT" in this method
   * @param tableName
   * @param columnNames
   * @param result
   * @return query string, use for logging
   * @throws SQLException
   */
  protected String executeUpsert(String tableName, List<String> columnNames, ExecutedInsertResult result) throws SQLException {
    throw new RuntimeException("Not implemented");
  }

  protected ColumnVisitor createColumnVisitor(PreparedStatement preparedStatement) {
    return new ColumnVisitor() {
      @Override
      public void booleanColumn(Column column) {
        try {
          logger.info(column.getName() + ": " + getPageReader().getBoolean(column));
          preparedStatement.setBoolean(column.getIndex() + 1, getPageReader().getBoolean(column));
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void longColumn(Column column) {
        try {
          logger.info(column.getName() + ": " + getPageReader().getLong(column));
          preparedStatement.setLong(column.getIndex() + 1, getPageReader().getLong(column));
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void doubleColumn(Column column) {
        try {
          logger.info(column.getName() + ": " + getPageReader().getDouble(column));
          preparedStatement.setDouble(column.getIndex() + 1, getPageReader().getDouble(column));
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void stringColumn(Column column) {
        try {
          logger.info(column.getName() + ": " + getPageReader().getString(column));
          preparedStatement.setString(column.getIndex() + 1, getPageReader().getString(column));
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void timestampColumn(Column column) {
        try {
          logger.info(column.getName() + ": " + getPageReader().getTimestampInstant(column));
          preparedStatement.setTimestamp(column.getIndex() + 1, Timestamp.from(getPageReader().getTimestampInstant(column)));
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void jsonColumn(Column column) {
        try {
          logger.info(column.getName() + ": " + getPageReader().getString(column));
          preparedStatement.setString(column.getIndex() + 1, getPageReader().getString(column));
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  protected class ExecutedInsertResult {
    int selectInsertRecordCount = 0;
    int selectUpdateRecordCount = 0;
    int selectUpsertRecordCount = 0;
  }
}
