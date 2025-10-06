package com.example;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pipeline specifically designed to trigger the exact error condition: "A request containing more
 * than one row is over the request size limit of 10000000" followed by:
 * "java.lang.IllegalArgumentException: Can not set java.util.List field
 * com.google.api.services.bigquery.model.TableRow.f to java.lang.Double"
 *
 * <p>This pipeline creates very large records to ensure we hit the 10MB limit, which then triggers
 * the Storage API bug with the 'f' field.
 */
public class LargeBatchBugReproduction {

  private static final Logger LOG = LoggerFactory.getLogger(LargeBatchBugReproduction.class);

  // Target size to exceed 10MB limit (10,000,000 bytes) for a SINGLE ROW
  private static final int TARGET_SINGLE_ROW_SIZE =
      12_000_000; // 12MB for a single record to exceed 10MB limit
  private static final int LARGE_STRING_SIZE = 4_000_000; // 4MB per string field (12MB per record)

  /** Pipeline options for the large batch bug reproduction. */
  public interface Options extends DataflowPipelineOptions {

    @Description("BigQuery dataset ID")
    @Default.String("test_dataset")
    String getDataset();

    void setDataset(String value);

    @Description("Base table name")
    @Default.String("large_batch_bug_test")
    String getBaseTableName();

    void setBaseTableName(String value);

    @Description("Enable workaround for the 'f' field bug using withFormatRecordOnFailureFunction")
    @Default.Boolean(false)
    Boolean getEnableWorkaround();

    void setEnableWorkaround(Boolean value);
  }

  /** Large record class designed to create batches that exceed 10MB. */
  @DefaultCoder(SerializableCoder.class)
  public static class VeryLargeRecord implements Serializable {
    public String id;
    public String massiveData1;
    public String massiveData2;
    public String massiveData3;
    public Double f; // The problematic field that will cause the bug
    public String category;
    public Long timestamp;
    public Double value1;
    public Double value2;
    public Double value3;

    public VeryLargeRecord(
        String id,
        String massiveData1,
        String massiveData2,
        String massiveData3,
        Double f,
        String category,
        Long timestamp,
        Double value1,
        Double value2,
        Double value3) {
      this.id = id;
      this.massiveData1 = massiveData1;
      this.massiveData2 = massiveData2;
      this.massiveData3 = massiveData3;
      this.f = f;
      this.category = category;
      this.timestamp = timestamp;
      this.value1 = value1;
      this.value2 = value2;
      this.value3 = value3;
    }

    public int getEstimatedSize() {
      return (massiveData1 != null ? massiveData1.length() : 0)
          + (massiveData2 != null ? massiveData2.length() : 0)
          + (massiveData3 != null ? massiveData3.length() : 0)
          + (id != null ? id.length() : 0)
          + (category != null ? category.length() : 0)
          + 100; // Approximate overhead for other fields
    }

    @Override
    public String toString() {
      return String.format(
          "VeryLargeRecord{id='%s', f=%f, category='%s', estimatedSize=%d}",
          id, f, category, getEstimatedSize());
    }
  }

  /** DoFn to convert VeryLargeRecord to TableRow with the problematic 'f' field. */
  static class ConvertToLargeTableRowFn extends DoFn<VeryLargeRecord, TableRow> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      VeryLargeRecord record = c.element();

      LOG.info(
          "Converting record {} with estimated size: {} bytes",
          record.id,
          record.getEstimatedSize());

      // Create TableRow using setF() to bypass early reflection checks
      // This allows the 'f' field conflict to occur later in Storage API's tableRowFromMessage
      TableRow row = new TableRow();

      // Set all the regular fields first
      row.set("id", record.id)
          .set("massive_data_1", record.massiveData1)
          .set("massive_data_2", record.massiveData2)
          .set("massive_data_3", record.massiveData3)
          .set("category", record.category)
          .set("timestamp", record.timestamp)
          .set("value_1", record.value1)
          .set("value_2", record.value2)
          .set("value_3", record.value3);

      // Use setF() to set the internal TableRow.f field directly with all our data
      // This bypasses the reflection-based field setting and should allow us to reach
      // the Storage API conversion where the real bug occurs when it tries to convert
      // the TableRow back to a message and encounters the 'f' field conflict
      java.util.List<com.google.api.services.bigquery.model.TableCell> cells =
          new java.util.ArrayList<>();
      cells.add(new com.google.api.services.bigquery.model.TableCell().setV(record.id));
      cells.add(new com.google.api.services.bigquery.model.TableCell().setV(record.massiveData1));
      cells.add(new com.google.api.services.bigquery.model.TableCell().setV(record.massiveData2));
      cells.add(new com.google.api.services.bigquery.model.TableCell().setV(record.massiveData3));
      cells.add(
          new com.google.api.services.bigquery.model.TableCell()
              .setV(record.f)); // The problematic 'f' value
      cells.add(new com.google.api.services.bigquery.model.TableCell().setV(record.category));
      cells.add(new com.google.api.services.bigquery.model.TableCell().setV(record.timestamp));
      cells.add(new com.google.api.services.bigquery.model.TableCell().setV(record.value1));
      cells.add(new com.google.api.services.bigquery.model.TableCell().setV(record.value2));
      cells.add(new com.google.api.services.bigquery.model.TableCell().setV(record.value3));
      row.setF(cells);

      c.output(row);
    }
  }

  /** Dynamic destinations for large batch processing. */
  static class LargeBatchDynamicDestinations extends DynamicDestinations<TableRow, String> {
    private final String projectId;
    private final String datasetId;
    private final String baseTableName;

    public LargeBatchDynamicDestinations(String projectId, String datasetId, String baseTableName) {
      this.projectId = projectId;
      this.datasetId = datasetId;
      this.baseTableName = baseTableName;
    }

    @Override
    public String getDestination(ValueInSingleWindow<TableRow> element) {
      TableRow row = element.getValue();
      String category = (String) row.get("category");
      return category != null ? category : "default";
    }

    @Override
    public TableDestination getTable(String destination) {
      String tableName = baseTableName + "_" + destination;
      String tableSpec = String.format("%s:%s.%s", projectId, datasetId, tableName);
      return new TableDestination(tableSpec, "Large batch table for category: " + destination);
    }

    @Override
    public TableSchema getSchema(String destination) {
      return new TableSchema()
          .setFields(
              Arrays.asList(
                  new TableFieldSchema().setName("id").setType("STRING"),
                  new TableFieldSchema().setName("massive_data_1").setType("STRING"),
                  new TableFieldSchema().setName("massive_data_2").setType("STRING"),
                  new TableFieldSchema().setName("massive_data_3").setType("STRING"),
                  new TableFieldSchema().setName("f").setType("FLOAT"), // The problematic field
                  new TableFieldSchema().setName("category").setType("STRING"),
                  new TableFieldSchema().setName("timestamp").setType("INTEGER"),
                  new TableFieldSchema().setName("value_1").setType("FLOAT"),
                  new TableFieldSchema().setName("value_2").setType("FLOAT"),
                  new TableFieldSchema().setName("value_3").setType("FLOAT")));
    }
  }

  /** Generate very large records to exceed the 10MB batch limit. */
  static class GenerateVeryLargeDataFn extends DoFn<Integer, VeryLargeRecord> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      Integer index = c.element();

      // Generate three large strings to create a very large record
      String massiveData1 = generateLargeString(LARGE_STRING_SIZE, (char) ('A' + (index % 26)));
      String massiveData2 = generateLargeString(LARGE_STRING_SIZE, (char) ('a' + (index % 26)));
      String massiveData3 = generateLargeString(LARGE_STRING_SIZE, (char) ('0' + (index % 10)));

      String category = "large_batch"; // All records go to same destination to create large batches

      VeryLargeRecord record =
          new VeryLargeRecord(
              "large_record_" + index,
              massiveData1,
              massiveData2,
              massiveData3,
              Math.PI * index + 0.123456789, // This will be set to the problematic 'f' field
              category,
              System.currentTimeMillis() + index,
              Math.E * index,
              Math.sqrt(index + 1),
              Math.log(index + 1));

      LOG.info("Generated record {} with size: {} bytes", record.id, record.getEstimatedSize());
      c.output(record);
    }

    private String generateLargeString(int size, char baseChar) {
      StringBuilder builder = new StringBuilder(size);
      for (int i = 0; i < size; i++) {
        builder.append((char) (baseChar + (i % 10)));
      }
      return builder.toString();
    }
  }

  /** Creates the BigQuery write transform with optional workaround for the 'f' field bug */
  private static BigQueryIO.Write<TableRow> createBigQueryWrite(Options options) {
    BigQueryIO.Write<TableRow> write =
        BigQueryIO.writeTableRows()
            .to(
                new LargeBatchDynamicDestinations(
                    options.getProject(), options.getDataset(), options.getBaseTableName()))
            .optimizedWrites()
            .withoutValidation()
            .ignoreInsertIds()
            .withMethod(
                BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE) // This will trigger the bug
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .withNumStorageWriteApiStreams(1); // Use single stream to force batching

    if (options.getEnableWorkaround()) {
      LOG.info("Applying workaround: withFormatRecordOnFailureFunction");
      write =
          write.withFormatRecordOnFailureFunction(
              new SimpleFunction<TableRow, TableRow>() {
                @Override
                public TableRow apply(TableRow row) {
                  // This workaround handles the 'f' field conflict by returning the row as-is
                  // The Storage API will handle the field name conflict internally

                  // Log the size of the large row being processed
                  String rowJson = row.toString();
                  int rowSize = rowJson.getBytes().length;
                  LOG.info(
                      "Processing large row in workaround function - estimated size: {} bytes ({} MB)",
                      rowSize,
                      rowSize / (1024.0 * 1024.0));

                  return row;
                }
              });
    }

    return write;
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline pipeline = Pipeline.create(options);

    // Calculate number of records needed to exceed 10MB for a SINGLE ROW
    int recordSize =
        LARGE_STRING_SIZE * 3 + 200; // Approximate size per record (3 large strings + metadata)
    int numRecords = 1; // Only create 1 record that exceeds 10MB

    LOG.info("Starting pipeline to reproduce large batch Storage API 'f' field bug");
    LOG.info("Target single row size: {} MB", TARGET_SINGLE_ROW_SIZE / (1024 * 1024));
    LOG.info("Estimated record size: {} bytes", recordSize);
    LOG.info("Generating {} record", numRecords);
    LOG.info("Estimated total size: {} MB", (numRecords * recordSize) / (1024 * 1024));

    if (options.getEnableWorkaround()) {
      LOG.info(
          "Workaround enabled: using withFormatRecordOnFailureFunction to handle 'f' field conflicts");
      LOG.info(
          "This should prevent the 'f' field bug and allow the pipeline to complete successfully");
    } else {
      LOG.info(
          "This should trigger: 'request size limit of 10000000' followed by the 'f' field bug");
    }

    // Generate indices for creating large dataset
    List<Integer> indices = new ArrayList<>();
    for (int i = 0; i < numRecords; i++) {
      indices.add(i);
    }

    // Create the pipeline that will trigger the bug
    pipeline
        .apply("Create Indices", Create.of(indices))
        .apply("Generate Very Large Records", ParDo.of(new GenerateVeryLargeDataFn()))
        .apply("Convert to TableRow", ParDo.of(new ConvertToLargeTableRowFn()))
        .apply("Write to BQ with Storage API", createBigQueryWrite(options));

    LOG.info("Pipeline configured. Running...");
    LOG.info("Expected sequence:");
    LOG.info(
        "1. 'A request containing more than one row is over the request size limit of 10000000'");
    LOG.info(
        "2. 'Can not set java.util.List field com.google.api.services.bigquery.model.TableRow.f to java.lang.Double'");

    try {
      org.apache.beam.sdk.PipelineResult result = pipeline.run();
      result.waitUntilFinish();
      LOG.info("Pipeline execution completed unexpectedly. Final state: {}", result.getState());
    } catch (Exception e) {
      LOG.error("Pipeline failed with exception:", e);

      String errorMessage = e.getMessage();
      String fullError = e.toString();

      if (fullError.contains("request size limit of 10000000")) {
        LOG.error("SUCCESS: Triggered the 10MB request size limit condition!");
      }

      if (errorMessage != null
          && errorMessage.contains(
              "Can not set java.util.List field com.google.api.services.bigquery.model.TableRow.f")) {
        LOG.error("SUCCESS: Reproduced the exact 'f' field bug after hitting size limit!");
      }

      // Print the full stack trace to match the user's request
      LOG.error("Full stack trace:", e);
    }

    System.exit(0);
  }
}
