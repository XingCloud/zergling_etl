package com.elex.bigdata.zergling.etl;

import static com.elex.bigdata.zergling.etl.ETLUtils.ip2Long;
import static com.elex.bigdata.zergling.etl.ETLUtils.truncateURL;

import com.elex.bigdata.logging.WrongTitleLogger;
import com.elex.bigdata.logging.WrongUIDLogger;
import com.elex.bigdata.logging.WrongURLLogger;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URLDecoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * User: Z J Wu Date: 14-2-20 Time: 下午4:15 Package: com.elex.bigdata.zergling.etl
 */
public class NavigatorETL extends ETLBase {
  private static final Logger LOGGER = Logger.getLogger(NavigatorETL.class);
  private String date;
  private String projectId;
  private String rawFilePath;
  private String output;
  private SimpleDateFormat sdf;

  public NavigatorETL(String date, String projectId, String rawFilePath, String output, SimpleDateFormat sdf) {
    this.date = date;
    this.projectId = projectId;
    this.rawFilePath = rawFilePath;
    this.output = output;
    this.sdf = sdf;
  }

  public void run() throws IOException, ParseException {
    File input = new File(rawFilePath);
    char c = '\t', blank = ' ', urlStop = '&';
    String sep1 = " - - ", sep2 = "/img.gif?";
    int a, b;
    Date d;
    String line, ip, dateString, urlParameters, uid, uidKey = "uid=", nationKey = "nation=", nation, titleKey = "title=", title, urlKey = "url=", url;
    int maxUID = 0;

    boolean writeURLAsTitle;
    String decodedURL;
    LOGGER.info("Begin to read.");
    long t1 = System.currentTimeMillis();
    try (BufferedReader br = new BufferedReader(
      new InputStreamReader(new FileInputStream(input))); PrintWriter pw = new PrintWriter(
      new OutputStreamWriter(new FileOutputStream(new File(output))))) {
      while ((line = br.readLine()) != null) {
        writeURLAsTitle = false;
        line = StringUtils.trimToNull(line);
        if (StringUtils.isBlank(line)) {
          continue;
        }
        a = line.indexOf(sep1);
        if (a < 0) {
          continue;
        }
        ip = line.substring(0, a);
        if (!ip.matches(ETLConstants.REGEX_IP_ADDRESS)) {
          continue;
        }

        a = line.indexOf('[');
        b = line.indexOf(']');
        if (a < 0 || b < 0) {
          continue;
        }
        dateString = line.substring(a + 1, b);
        d = sdf.parse(dateString);

        a = line.indexOf(sep2);
        b = line.indexOf(blank, a + sep2.length());
        urlParameters = line.substring(a + sep2.length(), b);

        uid = extractContent(urlParameters, uidKey, urlStop);
        if (StringUtils.isBlank(uid) || ETLConstants.NULL_STRING.equalsIgnoreCase(uid)) {
          WrongUIDLogger.log(line);
          continue;
        }
        if (uid.length() > maxUID) {
          maxUID = uid.length();
        }
        nation = extractContent(urlParameters, nationKey, urlStop);
        title = extractContent(urlParameters, titleKey, urlStop);
        url = truncateURL(extractContent(urlParameters, urlKey));

        pw.write(uid);
        pw.write(c);
        pw.write(ETLConstants.STANDARD_OUTPUT_SDF.format(d));
        pw.write(c);
        pw.write(String.valueOf(ip2Long(ip)));
        pw.write(c);
        if (StringUtils.isBlank(nation)) {
          pw.write(ETLConstants.UNKNOWN_NATION);
        } else {
          pw.write(nation.trim());
        }

        pw.write(c);
        if (StringUtils.isBlank(title)) {
          writeURLAsTitle = true;
        } else {
          try {
            pw.write(URLDecoder.decode(title.toLowerCase(), "utf8"));
          } catch (Exception e) {
            WrongTitleLogger.log(line);
            writeURLAsTitle = true;
          }
        }
        if (StringUtils.isNotBlank(url)) {
          try {
            decodedURL = URLDecoder.decode(url, "utf8");
          } catch (Exception e) {
            WrongURLLogger.log(line);
            decodedURL = ExceptedContent.ERROR_WRONG_URL.getId();
          }

          if (writeURLAsTitle) {
            pw.write(decodedURL);
          }
          pw.write(c);
          pw.write(decodedURL);
        }
        pw.write('\n');
      }
    }
    long t2 = System.currentTimeMillis();
    LOGGER.info("All done in " + (t2 - t1) + " millis.");
  }

  public static void main(String[] args) throws IOException, ParseException {
    if (args == null || args.length < 4) {
      LOGGER.info("Wrong parameter number.");
      System.exit(1);
    }
    String date = args[0];
    String projectId = args[1];
    String input = args[2];
    String output = args[3];
    NavigatorETL navigatorETL = new NavigatorETL(date, projectId, input, output,
                                                 new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH));
    navigatorETL.run();
  }
}
