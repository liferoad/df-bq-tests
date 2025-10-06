package com.example;

import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
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
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pipeline to reproduce the BigQuery TableRow bug with dynamic destinations and Storage API.
 *
 * <p>This pipeline demonstrates the exact error from production:
 * "java.lang.IllegalArgumentException: Can not set java.util.List field
 * com.google.api.services.bigquery.model.TableRow.f to java.lang.Double"
 *
 * <p>The error occurs when using BigQueryIO.writeTableRows() with dynamic destinations and Storage
 * API, where TableRow objects contain a field named 'f'.
 */
public class DynamicDestinationTableRowBugReproduction {

  private static final Logger LOG =
      LoggerFactory.getLogger(DynamicDestinationTableRowBugReproduction.class);

  /** Pipeline options for the dynamic destination TableRow bug reproduction. */
  public interface Options extends DataflowPipelineOptions {

    @Description("BigQuery dataset ID")
    @Default.String("test_dataset")
    String getDataset();

    void setDataset(String value);

    @Description("Base table name (will be suffixed with data type)")
    @Default.String("dynamic_tablerow_bug_test")
    String getBaseTableName();

    void setBaseTableName(String value);

    @Description("Enable workaround for TableRow 'f' column bug by using alternative field name")
    @Default.Boolean(false)
    Boolean getEnableWorkaround();

    void setEnableWorkaround(Boolean value);
  }

  /** Sample data class that will be converted to TableRow with problematic 'f' field. */
  @DefaultCoder(SerializableCoder.class)
  public static class SampleRecord implements Serializable {
    public String name;
    public Integer age;
    public Double f; // This field name 'f' will cause the bug
    public String category; // Used for dynamic destination routing

    public SampleRecord(String name, Integer age, Double f, String category) {
      this.name = name;
      this.age = age;
      this.f = f;
      this.category = category;
    }

    @Override
    public String toString() {
      return String.format(
          "SampleRecord{name='%s', age=%d, f=%f, category='%s'}", name, age, f, category);
    }
  }

  /** DoFn to convert SampleRecord to TableRow - this is where the bug will occur. */
  static class ConvertToTableRowFn extends DoFn<SampleRecord, TableRow> {
    private final boolean enableWorkaround;

    public ConvertToTableRowFn(boolean enableWorkaround) {
      this.enableWorkaround = enableWorkaround;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      SampleRecord record = c.element();

      TableRow row = new TableRow();
      if (enableWorkaround) {
        // Proper workaround: Use TableRow.setF() to set ALL row data as TableCell list
        // This completely bypasses the normal field setting mechanism
        row.setF(
            ImmutableList.of(
                new TableCell().setV(record.name),
                new TableCell().setV(record.age),
                new TableCell().setV(record.f),
                new TableCell().setV(record.category)));
        // Also set the category field normally so dynamic routing can access it
        row.set("category", record.category);
        LOG.info(
            "Using proper workaround: setting all fields via TableRow.setF() with TableCell list, category: {}",
            record.category);
      } else {
        // Create TableRow and set the problematic 'f' field
        // This will trigger the IllegalArgumentException
        row.set("name", record.name)
            .set("age", record.age)
            .set("f", record.f) // This line causes the bug!
            .set("category", record.category);
        LOG.info(
            "Setting 'f' field directly using .set() (will cause bug), category: {}",
            record.category);
      }

      c.output(row);
    }
  }

  /** Dynamic destinations implementation for routing records to different tables. */
  static class RecordDynamicDestinations extends DynamicDestinations<TableRow, String> {
    private final String projectId;
    private final String datasetId;
    private final String baseTableName;

    public RecordDynamicDestinations(String projectId, String datasetId, String baseTableName) {
      this.projectId = projectId;
      this.datasetId = datasetId;
      this.baseTableName = baseTableName;
    }

    @Override
    public String getDestination(ValueInSingleWindow<TableRow> element) {
      TableRow row = element.getValue();
      String category = (String) row.get("category");
      LOG.info("Routing record to destination: {}", category != null ? category : "default");
      return category != null ? category : "default";
    }

    @Override
    public TableDestination getTable(String destination) {
      String tableName = baseTableName + "_" + destination;
      String tableSpec = String.format("%s:%s.%s", projectId, datasetId, tableName);
      LOG.info("Routing to table: {}", tableSpec);
      return new TableDestination(tableSpec, "Table for category: " + destination);
    }

    @Override
    public TableSchema getSchema(String destination) {
      // Same schema for all destinations - includes the problematic 'f' field
      return new TableSchema()
          .setFields(
              Arrays.asList(
                  new TableFieldSchema().setName("name").setType("STRING"),
                  new TableFieldSchema().setName("age").setType("INTEGER"),
                  new TableFieldSchema().setName("f").setType("FLOAT"), // The problematic field
                  new TableFieldSchema().setName("category").setType("STRING")));
    }
  }

  public static void main(String[] args) {
    // Parse command line arguments
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline pipeline = Pipeline.create(options);

    // Create sample data that will trigger the bug
    List<SampleRecord> sampleData =
        Arrays.asList(
            new SampleRecord("Alice", 25, 1.5, "users"),
            new SampleRecord("Bob", 30, 2.7, "users"),
            new SampleRecord("Charlie", 35, 3.14, "admins"),
            new SampleRecord("Diana", 28, 4.2, "admins"),
            new SampleRecord("Eve", 32, 5.8, "guests"));

    LOG.info("Starting pipeline that will reproduce TableRow 'f' field bug");
    LOG.info("Using dynamic destinations with Storage API");
    LOG.info("Base table name: {}", options.getBaseTableName());
    LOG.info("Dataset: {}", options.getDataset());

    // Create the pipeline that will trigger the bug
    pipeline
        .apply("Create Sample Data", Create.of(sampleData))
        .apply(
            "Convert to TableRow", ParDo.of(new ConvertToTableRowFn(options.getEnableWorkaround())))
        .apply(
            "Write to BQ",
            BigQueryIO.writeTableRows()
                .to(
                    new RecordDynamicDestinations(
                        options.getProject(), options.getDataset(), options.getBaseTableName()))
                .optimizedWrites()
                .withoutValidation()
                .ignoreInsertIds()
                .withMethod(BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

    LOG.info("Pipeline configured. Running...");

    // Run the pipeline - this should fail with the IllegalArgumentException
    try {
      org.apache.beam.sdk.PipelineResult result = pipeline.run();
      result.waitUntilFinish();
      LOG.info("Pipeline execution completed. Final state: {}", result.getState());

      // Force JVM exit to prevent hanging due to BigQuery Storage API background threads
      System.exit(0);
    } catch (Exception e) {
      LOG.error("Pipeline failed with exception (this is expected for bug reproduction):", e);

      // Check if this is the specific error we're trying to reproduce
      if (e.getMessage() != null
          && e.getMessage()
              .contains(
                  "Can not set java.util.List field com.google.api.services.bigquery.model.TableRow.f")) {
        LOG.error("SUCCESS: Reproduced the exact TableRow 'f' field bug!");
        LOG.error("Error message: {}", e.getMessage());
      } else {
        LOG.error("Different error occurred: {}", e.getMessage());
      }

      // Force JVM exit even on exception to prevent hanging
      System.exit(1);
    }
  }
}
