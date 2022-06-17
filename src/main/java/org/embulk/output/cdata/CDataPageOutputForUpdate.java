package org.embulk.output.cdata;

import org.embulk.config.TaskReport;
import org.embulk.spi.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;
import java.util.stream.Collectors;

public class CDataPageOutputForUpdate implements TransactionalPageOutput {

  private static final Logger logger = LoggerFactory.getLogger(CDataPageOutputForUpdate.class);

  private final PageReader pageReader;
  private final Connection conn;
  private final CDataOutputPlugin.PluginTask task;
  private PreparedStatement preparedStatement;
  private String currentExternalColumnId = "";

  public CDataPageOutputForUpdate(final PageReader reader, Connection conn, CDataOutputPlugin.PluginTask task) {
    this.pageReader = reader;
    this.conn = conn;
    this.task = task;
    this.preparedStatement = null;
  }

  @Override
  public void add(Page page) {
    pageReader.setPage(page);

    ArrayList<String> columnNamesWithId = new ArrayList<>();
    columnNamesWithId.add("Id");
    ArrayList<String> columnNames = pageReader.getSchema().getColumns().stream()
      .map(Column::getName).collect(Collectors.toCollection(ArrayList::new));
    columnNamesWithId.addAll(columnNames);

    ArrayList<String> preparedValues = pageReader.getSchema().getColumns().stream()
      .map(it -> "?").collect(Collectors.toCollection(ArrayList::new));
    preparedValues.add("?"); // for Id

    HashMap<String, String> idMap = new HashMap<>();
    String selectStatement = "SELECT " + String.join(", ", columnNamesWithId) + " FROM " + task.getTable();
    logger.info(selectStatement);
    try {
      Statement statement = conn.createStatement();
      boolean ret = statement.execute(selectStatement);
      if (ret) {
        ResultSet rs = statement.getResultSet();
        while (rs.next()) {
          String id = "";
          String externalIdColumn = "";
          for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
            if (Objects.equals(rs.getMetaData().getColumnName(i), "Id")) {
              id = rs.getString(i);
            } else if (Objects.equals(rs.getMetaData().getColumnName(i), task.getExternalIdColumn())) {
              externalIdColumn = rs.getString(i);
            }
          }
          idMap.put(externalIdColumn, id);
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    String insertStatement = "INSERT INTO Temp#TEMP(" +
      String.join(", ", columnNamesWithId) +
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
              preparedStatementColumnLogger(column);
              preparedStatement.setBoolean(column.getIndex() + 2, pageReader.getBoolean(column));
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public void longColumn(Column column) {
            try {
              preparedStatementColumnLogger(column);
              preparedStatement.setLong(column.getIndex() + 2, pageReader.getLong(column));
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public void doubleColumn(Column column) {
            try {
              preparedStatementColumnLogger(column);
              preparedStatement.setDouble(column.getIndex() + 2, pageReader.getDouble(column));
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public void stringColumn(Column column) {
            try {
              preparedStatementColumnLogger(column);
              if (Objects.equals(column.getName(), task.getExternalIdColumn())) {
                currentExternalColumnId = pageReader.getString(column);
              }
              preparedStatement.setString(column.getIndex() + 2, pageReader.getString(column));
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public void timestampColumn(Column column) {
            try {
              preparedStatementColumnLogger(column);
              preparedStatement.setTimestamp(column.getIndex() + 2, Timestamp.from(pageReader.getTimestampInstant(column)));
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public void jsonColumn(Column column) {
            try {
              preparedStatementColumnLogger(column);
              preparedStatement.setString(column.getIndex() + 2, pageReader.getString(column));
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          }
        });
        preparedStatement.setString(1, idMap.get(currentExternalColumnId));
        preparedStatement.executeUpdate();
        logger.info("inserted to Temp#TEMP");
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
    try {
      String updateStatement = "UPDATE " + task.getTable() + " (" +
        String.join(", ", columnNamesWithId) +
        ") SELECT " +
        String.join(", ", columnNamesWithId) +
        " FROM Temp#TEMP";
      logger.info(updateStatement);
      conn.createStatement().executeUpdate(updateStatement, Statement.RETURN_GENERATED_KEYS);
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

  private void preparedStatementColumnLogger(Column column) {
    logger.info(column.getName() + ": " + pageReader.getString(column));
  }
}
