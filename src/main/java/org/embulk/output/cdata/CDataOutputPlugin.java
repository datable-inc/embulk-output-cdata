package org.embulk.output.cdata;

import org.embulk.config.*;
import org.embulk.spi.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

public class CDataOutputPlugin
  implements OutputPlugin {

  private static Connection conn;

  public interface PluginTask
    extends Task {
    @Config("driver_path")
    String getDriverPath();

    @Config("driver_class")
    String getDriverName();

    @Config("url")
    String getUrl();

    @Config("mode")
    String getMode();

    @Config("table")
    String getTable();

    @Config("external_id_column")
    String getExternalIdColumn();

    @Config("default_primary_key")
    @ConfigDefault("\"id\"")
    String getDefaultPrimaryKey();
  }

  @Override
  public ConfigDiff transaction(ConfigSource config,
                                Schema schema, int taskCount,
                                Control control) {
    PluginTask task = config.loadConfig(PluginTask.class);

    try {
      addDriverJarToClasspath(task.getDriverPath());
      Class.forName(task.getDriverName());
      conn = DriverManager.getConnection(task.getUrl());
    } catch (ClassNotFoundException | SQLException e) {
      throw new RuntimeException(e);
    }

    // retryable (idempotent) output:
    // return resume(task.dump(), schema, taskCount, control);

    // non-retryable (non-idempotent) output:
    control.run(task.dump());
    return Exec.newConfigDiff();
  }

  @Override
  public ConfigDiff resume(TaskSource taskSource,
                           Schema schema, int taskCount,
                           Control control) {
    throw new UnsupportedOperationException("cdata output plugin does not support resuming");
  }

  @Override
  public void cleanup(TaskSource taskSource,
                      Schema schema, int taskCount,
                      List<TaskReport> successTaskReports) {
  }

  @Override
  public TransactionalPageOutput open(TaskSource taskSource, Schema schema, int taskIndex) {
    PluginTask task = taskSource.loadTask(PluginTask.class);

    PageReader reader = new PageReader(schema);
    if (Objects.equals(task.getMode(), "insert")) {
      return new CDataPageOutputForInsert(reader, conn, task);
    }
    else if (Objects.equals(task.getMode(), "upsert")) {
      if (Objects.equals(task.getDriverName(), "cdata.jdbc.salesforce.SalesforceDriver")) {
        return new CDataPageOutputForUpsert(reader, conn, task);
      } else {
        return new CDataPageOutputForManualUpsert(reader, conn, task);
      }
    } else {
      return new CDataPageOutputForUpdate(reader, conn, task);
    }
  }

  private void addDriverJarToClasspath(String glob) {
    // TODO match glob
    final ClassLoader loader = getClass().getClassLoader();
    if (!(loader instanceof URLClassLoader)) {
      throw new RuntimeException("Plugin is not loaded by URLClassLoader unexpectedly.");
    }
    if (!"org.embulk.plugin.PluginClassLoader".equals(loader.getClass().getName())) {
      throw new RuntimeException("Plugin is not loaded by PluginClassLoader unexpectedly.");
    }
    Path path = Paths.get(glob);
    if (!path.toFile().exists()) {
      throw new ConfigException("The specified driver jar doesn't exist: " + glob);
    }
    final Method addPathMethod;
    try {
      addPathMethod = loader.getClass().getMethod("addPath", Path.class);
    } catch (final NoSuchMethodException ex) {
      throw new RuntimeException("Plugin is not loaded a ClassLoader which has addPath(Path), unexpectedly.");
    }
    try {
      addPathMethod.invoke(loader, Paths.get(glob));
    } catch (final IllegalAccessException ex) {
      throw new RuntimeException(ex);
    } catch (final InvocationTargetException ex) {
      final Throwable targetException = ex.getTargetException();
      if (targetException instanceof MalformedURLException) {
        throw new IllegalArgumentException(targetException);
      } else if (targetException instanceof RuntimeException) {
        throw (RuntimeException) targetException;
      } else {
        throw new RuntimeException(targetException);
      }
    }
  }
}
