package com.github.anora10.flink.rss.connector;

import java.util.*;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkRssConnectorTableSourceFactory implements DynamicTableSourceFactory {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkRssConnectorTableSourceFactory.class);

  public static final ConfigOption<String> URI = ConfigOptions.key("uri")
          .stringType()
          .noDefaultValue();

  // refresh interval in ms, default value 10 minutes
  public static final ConfigOption<Integer> REFRESH_INTERVAL = ConfigOptions.key("refresh-interval")
          .intType()
          .defaultValue(600000);

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {

    final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

    // discover a suitable decoding format
    final DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(
            DeserializationFormatFactory.class,
            FactoryUtil.FORMAT);

    // validate all options
    helper.validate();

    // get the validated options
    final ReadableConfig options = helper.getOptions();
    final String uri = options.get(URI);
    final Integer refreshInterval = options.get(REFRESH_INTERVAL);

    if (uri.isEmpty()) {
      LOG.warn("WARNING! Got empty string for required parameter {}", URI);
    }

    // split given uris into an array
    final String[] uris = uri.replaceAll("\\s+","").split(",");

    // derive the produced data type (excluding computed columns) from the catalog table
    final DataType producedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();

    // create and return dynamic table source
    return new FlinkRssConnectorTableSource(decodingFormat, producedDataType, uris, refreshInterval);
  }

  @Override
  public String factoryIdentifier() {
    return "rss-connector";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    final Set<ConfigOption<?>> options = new HashSet<>();
    options.add(URI);
    options.add(FactoryUtil.FORMAT);
    return options;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    final Set<ConfigOption<?>> options = new HashSet<>();
    options.add(REFRESH_INTERVAL);
    return options;
  }
}
