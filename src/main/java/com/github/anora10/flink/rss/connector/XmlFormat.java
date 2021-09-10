package com.github.anora10.flink.rss.connector;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.DynamicTableSource.DataStructureConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

public class XmlFormat implements DecodingFormat<DeserializationSchema<RowData>> {

  private String[] fieldNames;

  public XmlFormat(String[] fieldNames) {
    this.fieldNames = fieldNames;
  }

  @Override
  @SuppressWarnings("unchecked")
  public DeserializationSchema<RowData> createRuntimeDecoder(
      DynamicTableSource.Context context, DataType producedDataType) {
    final TypeInformation<RowData> producedTypeInfo =
        context.createTypeInformation(producedDataType);
    final DataStructureConverter converter = context.createDataStructureConverter(producedDataType);

    // create runtime class
    return new XmlDeserializer(converter, producedTypeInfo, fieldNames);
  }

  @Override
  public ChangelogMode getChangelogMode() {
    // this format can only produce INSERT rows
    return ChangelogMode.newBuilder().addContainedKind(RowKind.INSERT).build();
  }
}
