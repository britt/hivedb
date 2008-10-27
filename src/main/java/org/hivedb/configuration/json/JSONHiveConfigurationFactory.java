package org.hivedb.configuration.json;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hivedb.configuration.HiveConfiguration;
import org.hivedb.util.functional.Factory;
import org.hivedb.HiveRuntimeException;
import org.json.JSONObject;
import org.json.JSONException;

import java.io.FileInputStream;
import java.io.IOException;

//TODO test
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
      throw new HiveRuntimeException("Unable to load hive configuration file " + configurationFileName);
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
    FileInputStream file = new FileInputStream(configurationFileName);
    byte[] bytes = new byte[file.available()];
    file.read(bytes);
    file.close();
    return new String(bytes);
  }
}

