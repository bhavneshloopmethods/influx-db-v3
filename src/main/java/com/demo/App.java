package com.demo;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.write.Point;
import com.influxdb.v3.client.write.WriteOptions;

import io.github.cdimascio.dotenv.Dotenv;

public final class App {

    public static void main(final String[] args) throws Exception {
        Dotenv dotenv = Dotenv.load();

        String hostUrl = dotenv.get("INFLUX_HOST_URL");
        char[] authToken = dotenv.get("INFLUX_AUTH_TOKEN").toCharArray();

      
        InputStream inputStream = App.class.getClassLoader().getResourceAsStream("json-format.json");
        Object obj = new JSONParser().parse(new InputStreamReader(inputStream));
        JSONArray configArray = (JSONArray) obj;
        
        try (InfluxDBClient client = InfluxDBClient.getInstance(hostUrl, authToken, null)) {
            for (Object o : configArray) {
                if (o instanceof JSONObject) {
                    JSONObject config = (JSONObject) o;
                    String measurement = (String) config.get("measurement");

                    // Check if tags are JSON or String
                    Object tagsObj = config.get("tags");
                    Map<String, String> tags = new HashMap<>();
                    if (tagsObj instanceof JSONObject) {
                        tags = parseMap((JSONObject) tagsObj);
                    } else if (tagsObj instanceof String) {

                        // Assuming tags are comma-separated key=value pairs if it's a string
                        tags = parseTagsFromString((String) tagsObj);
                    }

                    Map<String, Double> fields = parseDoubleMap((JSONObject) config.get("fields"));
                    Map<String, Double> startValues = parseDoubleMap((JSONObject) config.get("startValues"));
                    Map<String, Double> minValues = parseDoubleMap((JSONObject) config.get("minValues"));
                    Map<String, Double> maxValues = parseDoubleMap((JSONObject) config.get("maxValues"));
                    Map<String, Double> variations = parseDoubleMap((JSONObject) config.get("variations"));
                    List<Long> range = (List<Long>) config.get("range");
                    String action = (String) config.get("action");
                    long counts = (long) config.get("counts");
                    long timeGaps = (long) config.get("timeGaps");
                    String bucket = (String) config.get("bucket");
                
                    
                    // Copy startValues to mutable currentValues
                    Map<String, Double> currentValues = new HashMap<>(startValues);

                    for (long i = 0; i < counts; i++) {
            
                        for (String key : fields.keySet()) {
                            Double variation = variations.get(key);
                            long minRange = range.get(0);
                            long maxRange = range.get(1);
                            if (i >= minRange && i <= maxRange) {
                                if (action.equals("increase")) {
                                    currentValues.put(key, Math.min(currentValues.get(key) + variation, maxValues.get(key)));
                                } else if (action.equals("decrease")) {
                                    currentValues.put(key, Math.max(currentValues.get(key) - variation, minValues.get(key)));
                                }
                            } else {
                                currentValues.put(key, currentValues.get(key) + (Math.random() * variation * 2 - variation));
                            }
                        }

                        Point point = Point.measurement(measurement);
                        tags.forEach(point::addTag);
                        currentValues.forEach((key, value) -> point.addField(key, value));

                        client.writePoint(point, new WriteOptions.Builder().database(bucket).build());

                        System.out.println("Data written: " + point.toLineProtocol());

                        // Sleep between writes
                        Thread.sleep(timeGaps * 1000);
                    }
                } else {
                    System.err.println("Unexpected non-JSONObject entry in configArray.");
                }
            }
        }

        System.out.println("Completed!!");
    }

    private static Map<String, String> parseMap(JSONObject jsonObject) {
        Map<String, String> map = new HashMap<>();
        if (jsonObject != null) {
            for (Object key : jsonObject.keySet()) {
                map.put(key.toString(), jsonObject.get(key).toString());
            }
        }
        return map;
    }

    // method to parse tags from a string
    private static Map<String, String> parseTagsFromString(String tagsString) {
        Map<String, String> map = new HashMap<>();
        String[] pairs = tagsString.split(",");
        for (String pair : pairs) {
            String[] keyValue = pair.split("=");
            if (keyValue.length == 2) {
                map.put(keyValue[0].trim(), keyValue[1].trim());
            }
        }
        return map;
    }

    private static Map<String, Double> parseDoubleMap(JSONObject jsonObject) {
        Map<String, Double> map = new HashMap<>();
        if (jsonObject != null) {
            for (Object key : jsonObject.keySet()) {
                map.put(key.toString(), Double.parseDouble(jsonObject.get(key).toString()));
            }
        }
        return map;
    }


}
