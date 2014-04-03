package com.elex.bigdata.zergling.etl;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClients;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * User: Z J Wu Date: 14-3-27 Time: 下午5:17 Package: com.elex.bigdata.zergling.etl
 */
public class UrlShortener {
  private static class InstanceHolder {
    private static final UrlShortener instance = new UrlShortener();
  }

  public synchronized static UrlShortener getInstance() {
    return InstanceHolder.instance;
  }

  private UrlShortener() {
    localCache = new HashMap<>(500);
  }

  private final HttpClient CLIENT = HttpClients.createDefault();
  private final String HTTP_SCHEMA = "http";
  private final String HOST = "restore.url";
  private final int PORT = 8080;
  private final String PATH = "/ur/r";

  private Map<String, String> localCache;

  // Concurrent is not important here, so ignore synchronization.
  public String toOriginalURL(String shortURL) throws Exception {
    String originalURL = localCache.get(shortURL);
    if (StringUtils.isNotBlank(originalURL)) {
      return originalURL;
    }
    URIBuilder builder = new URIBuilder();
    builder.setScheme(HTTP_SCHEMA);
    builder.setHost(HOST);
    builder.setPort(PORT);
    builder.setPath(PATH);
    builder.addParameter("url", shortURL);

    URI uri = builder.build();
    HttpGet httpGet = new HttpGet(uri);
    HttpResponse response = CLIENT.execute(httpGet);
    StatusLine statusLine = response.getStatusLine();
    int c = statusLine.getStatusCode();
    String result;
    if (200 == c) {
      result = StringUtils.trimToNull(readResponse(response.getEntity()));
      if (StringUtils.isBlank(result) || "err".equals(result)) {
        throw new Exception("Invalid url result - " + shortURL);
      } else {
        originalURL = result;
      }
    } else {
      throw new Exception("Error return code: " + String.valueOf(c));
    }
    localCache.put(shortURL, originalURL);
    return originalURL;
  }

  private String readResponse(HttpEntity responseEntity) throws IOException {
    if (responseEntity == null) {
      return null;
    }
    StringBuilder responseString = new StringBuilder();
    String line;
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(responseEntity.getContent()));) {
      while ((line = reader.readLine()) != null) {
        line = StringUtils.trimToNull(line);
        if (StringUtils.isNotEmpty(line)) {
          responseString.append(line);
        }
      }
    }
    return responseString.toString();
  }

  public static void main(String[] args) throws Exception {
    String path = "C:/Users/Administrator/Desktop/urls";
    File[] files = new File(path).listFiles();
    String line;
    Set<String> sortedSet = new TreeSet<>();
    UrlShortener shortener = UrlShortener.getInstance();
    for (File f : files) {
      if (f.isDirectory()) {
        continue;
      }
      System.out.println(f.getAbsolutePath());
      sortedSet.clear();
      try (BufferedReader br = new BufferedReader(
        new InputStreamReader(new FileInputStream(f))); PrintWriter pw = new PrintWriter(
        new OutputStreamWriter(new FileOutputStream(new File(f.getAbsolutePath() + ".original"))));) {
        while ((line = br.readLine()) != null) {
          line = StringUtils.trim(line);
          if (line.startsWith("http://goo.mx") || line.startsWith("https://goo.mx")) {
            sortedSet.add(shortener.toOriginalURL(line));
          } else {
            sortedSet.add(line);
          }
        }

        for (String s : sortedSet) {
          pw.write(s);
          pw.write('\n');
        }
      }
    }
  }
}
