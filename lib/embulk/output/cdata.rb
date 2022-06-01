Embulk::JavaPlugin.register_output(
  "cdata", "org.embulk.output.cdata.CDataOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
