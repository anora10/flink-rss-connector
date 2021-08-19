package com.github.anora10.flink.rss.connector;

import java.io.StringWriter;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import org.w3c.dom.Element;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

public class RssConnectorUtils {

  public static final String FAKER_DATETIME_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";

  private static DateTimeFormatter formatter =
      DateTimeFormatter.ofPattern(FAKER_DATETIME_FORMAT, new Locale("us"));

  public static String transformXmlToString(Element xmlElement)
  {
    String xmlString = null;
    TransformerFactory tf = TransformerFactory.newInstance();
    Transformer transformer;
    try {
      transformer = tf.newTransformer();

      // Uncomment if XML declaration not needed
      // transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");

      StringWriter writer = new StringWriter();
      //transform document to string
      transformer.transform(new DOMSource(xmlElement), new StreamResult(writer));
      xmlString = writer.getBuffer().toString();
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
    return xmlString;
  }
}
