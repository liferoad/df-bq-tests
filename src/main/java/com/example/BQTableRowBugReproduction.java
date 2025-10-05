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
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryStorageApiInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pipeline to reproduce the BigQuery TableRow bug with column named 'f'.
 *
 * <p>This pipeline demonstrates the issue described in: https://github.com/apache/beam/issues/33531
 *
 * <p>The bug occurs when trying to create a TableRow with a column named 'f' because TableRow
 * extends GenericJson and has a field named 'f' that expects a List<TableCell>, but we're trying to
 * set it to a String value.
 */
public class BQTableRowBugReproduction {

  private static final Logger LOG = LoggerFactory.getLogger(BQTableRowBugReproduction.class);

  /** Pipeline options for the BigQuery TableRow bug reproduction. */
  public interface Options extends DataflowPipelineOptions {

    @Description("BigQuery dataset ID")
    @Default.String("test_dataset")
    String getDataset();

    void setDataset(String value);

    @Description("BigQuery table ID")
    @Default.String("tablerow_bug_test")
    String getTable();

    void setTable(String value);

    @Description("Enable workaround for TableRow 'f' column bug by using alternative field name")
    @Default.Boolean(false)
    Boolean getEnableWorkaround();

    void setEnableWorkaround(Boolean value);
  }

  /** Sample data class with a field named 'f' that will cause the bug. */
  @DefaultCoder(SerializableCoder.class)
  public static class SampleData implements Serializable {
    public String name;
    public Integer age;
    public Double f; // This field name 'f' will cause the bug

    public SampleData(String name, Integer age, Double f) {
      this.name = name;
      this.age = age;
      this.f = f;
    }
  }

  /**
   * SerializableFunction to convert SampleData to TableRow. This follows the pattern shown in the
   * user's example with getTableRowFnc().
   */
  static org.apache.beam.sdk.transforms.SerializableFunction<SampleData, TableRow> getTableRowFnc(
      boolean enableWorkaround) {
    return (SampleData data) -> {
      TableRow row = new TableRow();
      if (enableWorkaround) {
        // Proper workaround: Use TableRow.setF() to set ALL row data as TableCell list
        // This completely bypasses the normal field setting mechanism
        row.setF(
            ImmutableList.of(
                new TableCell().setV(data.name),
                new TableCell().setV(data.age),
                new TableCell().setV(data.f)));
        LOG.info(
            "Using proper workaround: setting all fields via TableRow.setF() with TableCell list");
      } else {
        // This is where the bug occurs - trying to set field 'f' on TableRow using .set()
        row.set("name", data.name)
            .set("age", data.age)
            .set("f", data.f); // This line will cause the bug
        LOG.info("Setting 'f' field directly using .set() (will cause bug)");
      }
      return row;
    };
  }

  public static void main(String[] args) {
    // Parse command line arguments
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline pipeline = Pipeline.create(options);

    // Create sample data with the problematic 'f' field
    // Include some data that will cause Storage API insert failures
    List<SampleData> sampleData =
        Arrays.asList(
            new SampleData("Alice", 25, 1.5),
            new SampleData("Bob", 30, 2.7),
            new SampleData("Charlie", 35, 3.14));

    // Define BigQuery table schema
    // Schema is the same regardless of workaround - the difference is in how TableRow is populated
    TableSchema schema =
        new TableSchema()
            .setFields(
                Arrays.asList(
                    new TableFieldSchema().setName("name").setType("STRING"),
                    new TableFieldSchema().setName("age").setType("INTEGER"),
                    new TableFieldSchema().setName("f").setType("FLOAT")));

    if (options.getEnableWorkaround()) {
      LOG.info("Using proper TableRow.setF() workaround with TableCell list structure");
    } else {
      LOG.info("This pipeline will demonstrate the TableRow 'f' column bug using .set() method");
    }

    // Build the table reference
    String tableSpec =
        String.format("%s:%s.%s", options.getProject(), options.getDataset(), options.getTable());

    LOG.info("Writing to BigQuery table: {}", tableSpec);
    if (options.getEnableWorkaround()) {
      LOG.info("Workaround enabled: using proper TableRow.setF() method with TableCell list");
    } else {
      LOG.info("This pipeline will demonstrate the TableRow 'f' column bug using .set() method");
    }

    // Apply the BigQuery write operation
    WriteResult writeResult =
        pipeline
            .apply("Create Sample Data", Create.of(sampleData))
            .apply(
                "Write to BigQuery",
                BigQueryIO.<SampleData>write()
                    .to(tableSpec)
                    .withSchema(schema)
                    .withFormatFunction(getTableRowFnc(options.getEnableWorkaround()))
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                    .withMethod(
                        BigQueryIO.Write.Method
                            .STORAGE_WRITE_API) // Using Storage Write API as requested
                );

    // Process failed Storage API inserts if available
    try {
      if (writeResult.getFailedStorageApiInserts() != null) {
        LOG.info("Failed Storage API inserts PCollection is available - processing errors");

        // Add a transform to log failed inserts
        writeResult
            .getFailedStorageApiInserts()
            .apply(
                "Log Failed Inserts",
                ParDo.of(
                    new DoFn<BigQueryStorageApiInsertError, Void>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        BigQueryStorageApiInsertError error = c.element();
                        LOG.error("BigQuery Storage API Insert Failed:");
                        LOG.error("  Row: {}", error.getRow());
                        LOG.error("  Error Message: {}", error.getErrorMessage());
                      }
                    }));
      } else {
        LOG.info("No failed Storage API inserts PCollection available");
      }
    } catch (Exception e) {
      LOG.info("Failed inserts not supported with current write method: {}", e.getMessage());
    }

    // Run the pipeline and wait for completion
    org.apache.beam.sdk.PipelineResult result = pipeline.run();
    result.waitUntilFinish();

    LOG.info("Pipeline execution completed. Final state: {}", result.getState());
  }
}
