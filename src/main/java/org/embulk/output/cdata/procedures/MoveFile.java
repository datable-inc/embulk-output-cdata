package org.embulk.output.cdata.procedures;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Arrays;

public class MoveFile {

  public static String[] SUPPOERTED_CLASS_NAMES = { "cdata.jdbc.csv.CSVDriver" };

  public static void execute(Connection conn, String fileName) throws SQLException {
    String className = conn.getMetaData().getDriverName();
    if (Arrays.asList(SUPPOERTED_CLASS_NAMES).contains(className)) {
      System.out.println("skipped move file: " + className);
      return;
    }
    Statement stmt = conn.createStatement();
    stmt.execute("EXEC MOVEFILE @SourcePath='" + fileName + "' @DestinationPath='" + new SimpleDateFormat("yyyyMMddHHmmss").format(new java.util.Date()) + "'");
  }
}
