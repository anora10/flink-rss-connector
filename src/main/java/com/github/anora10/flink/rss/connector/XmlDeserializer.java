package com.github.anora10.flink.rss.connector;

import java.io.ByteArrayInputStream;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.RuntimeConverter.Context;
import org.apache.flink.table.connector.source.DynamicTableSource.DataStructureConverter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

public class XmlDeserializer implements DeserializationSchema<RowData> {

  private static final Logger LOG = LoggerFactory.getLogger(XmlDeserializer.class);

  private String[] fieldNames;

  private final DataStructureConverter converter;
  private final TypeInformation<RowData> producedTypeInfo;

  public XmlDeserializer(
      DataStructureConverter converter,
      TypeInformation<RowData> producedTypeInfo,
      String[] fieldNames) {
    this.converter = converter;
    this.producedTypeInfo = producedTypeInfo;
    this.fieldNames = fieldNames;
  }

  @Override
  public TypeInformation<RowData> getProducedType() {
    return producedTypeInfo;
  }

  @Override
  public void open(InitializationContext context) {
    converter.open(Context.create(XmlDeserializer.class.getClassLoader()));
  }

  @Override
  public RowData deserialize(byte[] message) {
    GenericRowData row = new GenericRowData(fieldNames.length);
    try {
      // parse XML item from message byte array
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = factory.newDocumentBuilder();
      ByteArrayInputStream byteMessage = new ByteArrayInputStream(message);
      Document document = builder.parse(byteMessage);
      document.getDocumentElement().normalize();
      Element itemElement = document.getDocumentElement();

      // set queried row fields
      for (int i = 0; i < fieldNames.length; i++) {
        Node fieldNode = itemElement.getElementsByTagName(fieldNames[i]).item(0);
        row.setField(i, StringData.fromString(fieldNode == null ? "" : fieldNode.getTextContent()));
      }
    } catch (Exception e) {
      LOG.error("Error while deserializing message", e);
    }
    return row;
  }

  @Override
  public boolean isEndOfStream(RowData nextElement) {
    return false;
  }
}
