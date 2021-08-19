package com.github.anora10.flink.rss.connector;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkRssConnectorTableSource implements ScanTableSource {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkRssConnectorTableSource.class);

  private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
  private final DataType producedDataType;
  private final String[] uris;
  private final Integer refreshInterval;

  public FlinkRssConnectorTableSource (DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
                                       DataType producedDataType,
                                       String[] uris,
                                       Integer refreshInterval) {
    this.decodingFormat = decodingFormat;
    this.producedDataType = producedDataType;
    this.uris = uris;
    this.refreshInterval = refreshInterval;
  }

  @Override
  public DynamicTableSource copy() {
    return new FlinkRssConnectorTableSource(decodingFormat, producedDataType, uris, refreshInterval);
  }

  @Override
  public String asSummaryString() {
    return "RSS Connector Table Source";
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return decodingFormat.getChangelogMode();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
    final DeserializationSchema<RowData> deserializer = decodingFormat.createRuntimeDecoder(
            runtimeProviderContext,
            producedDataType);

    final SourceFunction<RowData> sourceFunction = new FlinkRssConnectorSourceFunction(
            deserializer, uris, refreshInterval);

    return SourceFunctionProvider.of(sourceFunction, false);
  }
}
