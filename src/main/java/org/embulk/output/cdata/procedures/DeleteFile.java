package org.embulk.output.cdata.procedures;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

public class DeleteFile {

  private static final Logger logger = LoggerFactory.getLogger(DeleteFile.class);
  public static String[] SUPPOERTED_CLASS_NAMES = { "cdata.jdbc.csv.CSVDriver" };

  public static void execute(Connection conn, String fileName) throws SQLException {
    String className = conn.getMetaData().getDriverName();
    if (Arrays.asList(SUPPOERTED_CLASS_NAMES).contains(className)) {
      logger.info("not supported delete file className, skipped delete file: " + className);
      return;
    }
    Statement stmt = conn.createStatement();
    String source = fileName + ".csv";
    String sql = "EXEC DELETEFILE @PATH='" + source + "'";
    try {
      boolean ret = stmt.execute(sql);
      if (!ret) {
        logger.info("failed to delete file: " + source);
      }
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }
}
