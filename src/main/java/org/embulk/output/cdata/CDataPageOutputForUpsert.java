package org.embulk.output.cdata;

import org.embulk.config.TaskReport;
import org.embulk.spi.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.stream.Collectors;

public class CDataPageOutputForUpsert implements TransactionalPageOutput {

  private static final Logger logger = LoggerFactory.getLogger(CDataPageOutputForUpsert.class);

  private final PageReader pageReader;
  private final Connection conn;
  private final CDataOutputPlugin.PluginTask task;
  private PreparedStatement preparedStatement;

  public CDataPageOutputForUpsert(final PageReader reader, Connection conn, CDataOutputPlugin.PluginTask task) {
    this.pageReader = reader;
    this.conn = conn;
    this.task = task;
    this.preparedStatement = null;
  }

  @Override
  public void add(Page page) {
    pageReader.setPage(page);
    ArrayList<String> columnNames = pageReader.getSchema().getColumns().stream()
      .map(Column::getName).collect(Collectors.toCollection(ArrayList::new));
    columnNames.add("ExternalIdColumn");

    ArrayList<String> preparedValues = pageReader.getSchema().getColumns().stream()
      .map(it -> "?").collect(Collectors.toCollection(ArrayList::new));
    preparedValues.add("?"); // for ExternalIdColumn

    String insertStatement = "INSERT INTO Temp#TEMP(" +
      String.join(", ", columnNames) +
      ") VALUES (" +
      String.join(", ", preparedValues) + ")";
    logger.info(insertStatement);

    while (pageReader.nextRecord()) {
      try {
        this.preparedStatement = conn.prepareStatement(insertStatement, Statement.RETURN_GENERATED_KEYS);

        pageReader.getSchema().visitColumns(new ColumnVisitor() {
          @Override
          public void booleanColumn(Column column) {
            try {
              logger.info(column.getName() + ": " + pageReader.getBoolean(column));
              preparedStatement.setBoolean(column.getIndex() + 1, pageReader.getBoolean(column));
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public void longColumn(Column column) {
            try {
              logger.info(column.getName() + ": " + pageReader.getLong(column));
              preparedStatement.setLong(column.getIndex() + 1, pageReader.getLong(column));
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public void doubleColumn(Column column) {
            try {
              logger.info(column.getName() + ": " + pageReader.getDouble(column));
              preparedStatement.setDouble(column.getIndex() + 1, pageReader.getDouble(column));
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public void stringColumn(Column column) {
            try {
              logger.info(column.getName() + ": " + pageReader.getString(column));
              preparedStatement.setString(column.getIndex() + 1, pageReader.getString(column));
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public void timestampColumn(Column column) {
            try {
              logger.info(column.getName() + ": " + pageReader.getTimestampInstant(column));
              preparedStatement.setTimestamp(column.getIndex() + 1, Timestamp.from(pageReader.getTimestampInstant(column)));
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public void jsonColumn(Column column) {
            try {
              logger.info(column.getName() + ": " + pageReader.getString(column));
              preparedStatement.setString(column.getIndex() + 1, pageReader.getString(column));
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          }
        });
        preparedStatement.setString(preparedValues.size(), task.getExternalIdColumn());
        preparedStatement.executeUpdate();
        logger.info("inserted to Temp#TEMP");
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
    try {
      String upserStatement = "UPSERT INTO " + task.getTable() + " (" +
        String.join(", ", columnNames) +
        ") SELECT " +
        String.join(", ", columnNames) +
        " FROM Temp#TEMP";
      logger.info(upserStatement);
      conn.createStatement().executeUpdate(upserStatement, Statement.RETURN_GENERATED_KEYS);
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
}
