package org.embulk.output.cdata;

import org.embulk.config.TaskReport;
import org.embulk.spi.*;

import java.sql.*;

public class CDataPageOutput implements TransactionalPageOutput {

  private final PageReader pageReader;
  private final Connection conn;
  private final CDataOutputPlugin.PluginTask task;
  private PreparedStatement preparedStatement;

  public CDataPageOutput(final PageReader reader, Connection conn, CDataOutputPlugin.PluginTask task) {
    this.pageReader = reader;
    this.conn = conn;
    this.task = task;
    this.preparedStatement = null;
  }

  @Override
  public void add(Page page) {
    pageReader.setPage(page);
    String[] columnNames = pageReader.getSchema().getColumns().stream().map(Column::getName).toArray(String[]::new);
    String[] preparedValues = pageReader.getSchema().getColumns().stream().map(it -> "?").toArray(String[]::new);

    while (pageReader.nextRecord()) {
      String query = "INSERT INTO Temp#TEMP(" +
        String.join(", ", columnNames) +
        ") VALUES (" +
        String.join(", ", preparedValues) + ")";

      try {
        this.preparedStatement = conn.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);

        pageReader.getSchema().visitColumns(new ColumnVisitor() {
          @Override
          public void booleanColumn(Column column) {
            try {
              preparedStatement.setBoolean(column.getIndex(), pageReader.getBoolean(column));
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public void longColumn(Column column) {
            try {
              preparedStatement.setLong(column.getIndex() + 1, pageReader.getLong(column));
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public void doubleColumn(Column column) {
            try {
              preparedStatement.setDouble(column.getIndex() + 1, pageReader.getDouble(column));
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public void stringColumn(Column column) {
            try {
              preparedStatement.setString(column.getIndex() + 1, pageReader.getString(column));
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public void timestampColumn(Column column) {
            try {
              preparedStatement.setTimestamp(column.getIndex() + 1, Timestamp.from(pageReader.getTimestampInstant(column)));
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public void jsonColumn(Column column) {
            try {
              preparedStatement.setString(column.getIndex() + 1, pageReader.getString(column));
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          }
        });
        preparedStatement.executeUpdate();
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
    try {
      String query = "UPSERT INTO " + task.getTable() + " (" +
        String.join(", ", columnNames) +
        ") SELECT " +
        String.join(", ", columnNames) +
        " FROM Temp#TEMP";
      conn.createStatement().executeUpdate(query, Statement.RETURN_GENERATED_KEYS);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void finish() {
  }

  @Override
  public void close() {
  }

  @Override
  public void abort() {
  }

  @Override
  public TaskReport commit() {
    return null;
  }
}
