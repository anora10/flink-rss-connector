package com.github.anora10.flink.rss.connector;

import java.util.Collections;
import java.util.Set;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.utils.TableSchemaUtils;

public class XmlFormatFactory implements DeserializationFormatFactory {

  // no options defined

  @Override
  public String factoryIdentifier() {
    return "xml";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Collections.emptySet();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Collections.emptySet();
  }

  @Override
  public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
      DynamicTableFactory.Context context, ReadableConfig formatOptions) {

    FactoryUtil.validateFactoryOptions(this, formatOptions);

    CatalogTable catalogTable = context.getCatalogTable();
    String[] fieldNames =
        TableSchemaUtils.getPhysicalSchema(catalogTable.getSchema()).getFieldNames();

    // validate data types: only strings can be present in schema
    DataType[] fieldTypes =
        TableSchemaUtils.getPhysicalSchema(catalogTable.getSchema()).getFieldDataTypes();
    for (DataType dataType : fieldTypes) {
      if (dataType.getLogicalType().getTypeRoot() != LogicalTypeRoot.VARCHAR)
        throw new ValidationException(
            "Only String types are allowed in table schema. "
                + dataType.getLogicalType().getTypeRoot().toString()
                + " is found. ");
    }

    return new XmlFormat(fieldNames);
  }
}
