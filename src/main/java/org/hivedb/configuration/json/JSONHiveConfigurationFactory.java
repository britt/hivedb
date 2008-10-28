package org.hivedb.configuration.json;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.HiveRuntimeException;
import org.hivedb.configuration.HiveConfiguration;
import org.hivedb.util.functional.Factory;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class JSONHiveConfigurationFactory implements Factory<HiveConfiguration> {
  private final static Log log = LogFactory.getLog(JSONHiveConfigurationFactory.class);
  private String configurationFileName;
  private HiveConfigurationJSONSerializer serializer = new HiveConfigurationJSONSerializer();

  public JSONHiveConfigurationFactory(String configurationFileName) {
    this.configurationFileName = configurationFileName;
  }

  public HiveConfiguration newInstance() {
    String json = null;
    try {
      json = readFile();
    } catch (IOException e) {
      throw new IllegalStateException("Unable to load hive configuration file " + configurationFileName);
    }

    HiveConfiguration configuration = null;
    try {
      configuration = serializer.loadFromJSON(new JSONObject(json));
    } catch (JSONException e) {
      throw new HiveRuntimeException("JSON PARSING ERROR: Configuration file was in an invalid format.");
    }
    return configuration;
  }

  private String readFile() throws IOException {
    String absolutePath = new File(configurationFileName).getAbsolutePath();
    FileInputStream file = new FileInputStream(absolutePath);
    byte[] bytes = new byte[file.available()];
    file.read(bytes);
    file.close();
    return new String(bytes);
  }
}

