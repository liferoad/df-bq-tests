# BigQuery TableRow Bug Reproduction

This project reproduces the bug described in [Apache Beam Issue #33531](https://github.com/apache/beam/issues/33531) where BigQuery TableRow does not accept a column named 'f'.

## Problem Description

When working with a BigQuery table that has a column named 'f', construction of a TableRow object fails with:

```
java.lang.IllegalArgumentException: Can not set java.util.List field com.google.api.services.bigquery.model.TableRow.f to java.lang.Double
```

This happens because TableRow extends GenericJson and has a field named 'f' that expects a `List<TableCell>`, but the pipeline tries to set it to a Double value.

## Project Structure

```
df-bq-tests/
├── pom.xml                                           # Maven configuration with Beam dependencies
├── src/main/java/com/example/
│   ├── BQTableRowBugReproduction.java               # Original pipeline with format functions
│   ├── DynamicDestinationTableRowBugReproduction.java # Dynamic destinations pipeline
│   └── LargeBatchBugReproduction.java               # Large batch pipeline for Storage API bug
└── README.md                                        # This file
```

## Prerequisites

1. Java 11 or higher
2. Maven 3.6 or higher
3. Google Cloud Platform account with BigQuery enabled
4. GCP credentials configured (via `gcloud auth application-default login` or service account key)

## Setup Instructions

1. **Clone or navigate to this directory:**
   ```bash
   cd df-bq-tests
   ```

2. **Set up GCP authentication:**
   ```bash
   gcloud auth application-default login
   # OR set GOOGLE_APPLICATION_CREDENTIALS environment variable to your service account key file
   ```

3. **Build the project:**
   ```bash
   mvn clean compile
   ```

## Running the Pipeline

This project contains three different pipeline implementations that reproduce the same TableRow 'f' field bug:

### 1. Original Pipeline (BQTableRowBugReproduction)
Uses `BigQueryIO.write()` with format functions and static table destinations.

#### Test the Bug (Default Behavior)
To reproduce the TableRow 'f' column bug, run:

```bash
mvn compile exec:java \
  -Dexec.mainClass=com.example.BQTableRowBugReproduction \
  -Dexec.args="--project=manav-jit-test \
               --dataset=test \
               --table=tablerow_bug_test \
               --runner=DirectRunner"
```

#### Test with Dataflow Runner
```bash
mvn compile exec:java \
  -Dexec.mainClass=com.example.BQTableRowBugReproduction \
  -Dexec.args="--project=manav-jit-test \
               --dataset=test \
               --table=tablerow_bug_test \
               --runner=DataflowRunner \
               --region=us-central1 \
               --tempLocation=gs://tmp_xqhu/temp"
```

#### Test with Workaround Enabled
To test the workaround that avoids the bug by using an alternative field name:

```bash
mvn compile exec:java \
  -Dexec.mainClass=com.example.BQTableRowBugReproduction \
  -Dexec.args="--project=manav-jit-test \
               --dataset=test \
               --table=tablerow_bug_test_workaround \
               --runner=DirectRunner \
               --enableWorkaround=true"
```

#### Test Workaround with Dataflow Runner
```bash
mvn compile exec:java \
  -Dexec.mainClass=com.example.BQTableRowBugReproduction \
  -Dexec.args="--project=manav-jit-test \
               --dataset=test \
               --table=tablerow_bug_test_workaround \
               --runner=DataflowRunner \
               --region=us-central1 \
               --tempLocation=gs://tmp_xqhu/temp \
               --enableWorkaround=true"
```

### 2. Dynamic Destinations Pipeline (DynamicDestinationTableRowBugReproduction)
Uses `BigQueryIO.writeTableRows()` with dynamic destinations, optimized writes, and Storage API. This reproduces the exact production scenario.

#### Test the Bug with Dynamic Destinations
```bash
mvn compile exec:java \
  -Dexec.mainClass=com.example.DynamicDestinationTableRowBugReproduction \
  -Dexec.args="--project=manav-jit-test \
               --dataset=test \
               --baseTableName=dynamic_tablerow_bug_test \
               --runner=DirectRunner"
```

#### Test with Dataflow Runner
```bash
mvn compile exec:java \
  -Dexec.mainClass=com.example.DynamicDestinationTableRowBugReproduction \
  -Dexec.args="--project=manav-jit-test \
               --dataset=test \
               --baseTableName=dynamic_tablerow_bug_test \
               --runner=DataflowRunner \
               --region=us-central1 \
               --tempLocation=gs://tmp_xqhu/temp"
```

### 3. Large Batch Pipeline (LargeBatchBugReproduction)
Uses `BigQueryIO.writeTableRows()` with Storage API and generates large batches to trigger the specific Storage API bug.

#### Test the Storage API Bug
```bash
mvn compile exec:java \
  -Dexec.mainClass=com.example.LargeBatchBugReproduction \
  -Dexec.args="--project=manav-jit-test \
               --dataset=test \
               --baseTableName=large_batch_bug_test \
               --runner=DirectRunner"
```

#### Test with Dataflow Runner
```bash
mvn compile exec:java \
  -Dexec.mainClass=com.example.LargeBatchBugReproduction \
  -Dexec.args="--project=manav-jit-test \
               --dataset=test \
               --baseTableName=large_batch_bug_test \
               --runner=DataflowRunner \
               --region=us-central1 \
               --tempLocation=gs://tmp_xqhu/temp"
```

#### Test with Workaround Enabled (Large Batch Pipeline)
To test the workaround that prevents the Storage API 'f' field bug using `withFormatRecordOnFailureFunction`:

```bash
mvn compile exec:java \
  -Dexec.mainClass=com.example.LargeBatchBugReproduction \
  -Dexec.args="--project=manav-jit-test \
               --dataset=test \
               --baseTableName=large_batch_bug_test_workaround \
               --runner=DirectRunner \
               --enableWorkaround=true"
```

#### Test Workaround with Dataflow Runner (Large Batch Pipeline)
```bash
mvn compile exec:java \
  -Dexec.mainClass=com.example.LargeBatchBugReproduction \
  -Dexec.args="--project=manav-jit-test \
               --dataset=test \
               --baseTableName=large_batch_bug_test_workaround \
               --runner=DataflowRunner \
               --region=us-central1 \
               --tempLocation=gs://tmp_xqhu/temp \
               --enableWorkaround=true"
```

## BigQuery Storage API 'f' Field Bug Reproduction

The `LargeBatchBugReproduction` pipeline is specifically designed to reproduce a BigQuery Storage API bug that occurs when using dynamic destinations with TableRow objects containing a field named 'f'.

### The Bug

The bug manifests as the following error sequence:

1. First, a warning about request size:
   ```
   A request containing more than one row is over the request size limit of 10000000. This is unexpected. All rows in the request will be sent to the failed-rows PCollection.
   ```

2. Then, the actual exception:
   ```
   java.lang.IllegalArgumentException: Can not set java.util.List field com.google.api.services.bigquery.model.TableRow.f to java.lang.Double
   ```

### Root Cause

The bug occurs because:
1. BigQuery Storage API processes records in batches
2. When a batch exceeds 10MB, it triggers special handling
3. During this handling, the Storage API tries to access the internal `f` field of TableRow (which is a `List<TableCell>`)
4. If your data also has a field named 'f', there's a conflict that causes the IllegalArgumentException

### Expected Stack Trace

When the bug is triggered, you should see a stack trace similar to:

```
java.lang.IllegalArgumentException: Can not set java.util.List field com.google.api.services.bigquery.model.TableRow.f to java.lang.Double
    at java.base/jdk.internal.reflect.UnsafeFieldAccessorImpl.throwSetIllegalArgumentException(UnsafeFieldAccessorImpl.java:167)
    at java.base/jdk.internal.reflect.UnsafeFieldAccessorImpl.throwSetIllegalArgumentException(UnsafeFieldAccessorImpl.java:171)
    at java.base/jdk.internal.reflect.UnsafeObjectFieldAccessorImpl.set(UnsafeObjectFieldAccessorImpl.java:81)
    at java.base/java.lang.reflect.Field.set(Field.java:780)
    at com.google.api.client.util.FieldInfo.setFieldValue(FieldInfo.java:281)
    at com.google.api.client.util.FieldInfo.setValue(FieldInfo.java:237)
    at com.google.api.client.util.GenericData.put(GenericData.java:98)
    at org.apache.beam.sdk.io.gcp.bigquery.TableRowToStorageApiProto.tableRowFromMessage(TableRowToStorageApiProto.java:1115)
    at org.apache.beam.sdk.io.gcp.bigquery.TableRowToStorageApiProto.jsonValueFromMessageValue(TableRowToStorageApiProto.java:1140)
    at org.apache.beam.sdk.io.gcp.bigquery.TableRowToStorageApiProto.tableRowFromMessage(TableRowToStorageApiProto.java:1117)
    at org.apache.beam.sdk.io.gcp.bigquery.TableRowToStorageApiProto.jsonValueFromMessageValue(TableRowToStorageApiProto.java:1140)
    at org.apache.beam.sdk.io.gcp.bigquery.TableRowToStorageApiProto.tableRowFromMessage(TableRowToStorageApiProto.java:1117)
    at org.apache.beam.sdk.io.gcp.bigquery.TableRowToStorageApiProto.tableRowFromMessage(TableRowToStorageApiProto.java:1098)
    at org.apache.beam.sdk.io.gcp.bigquery.StorageApiWriteUnshardedRecords$WriteRecordsDoFn$DestinationState.flush(StorageApiWriteUnshardedRecords.java:655)
    at org.apache.beam.sdk.io.gcp.bigquery.StorageApiWriteUnshardedRecords$WriteRecordsDoFn.flushAll(StorageApiWriteUnshardedRecords.java:1042)
    at org.apache.beam.sdk.io.gcp.bigquery.StorageApiWriteUnshardedRecords$WriteRecordsDoFn.finishBundle(StorageApiWriteUnshardedRecords.java:1207)
```

### Notes

- The `LargeBatchBugReproduction` pipeline is designed to fail with the specific error - this is expected behavior for bug reproduction
- The bug occurs with Storage API (`STORAGE_API_AT_LEAST_ONCE` method) and dynamic destinations
- Use this pipeline for the most reliable reproduction of the exact error condition

## Expected Error

Both pipelines will fail with the same error when trying to set a field named 'f' on a TableRow:

```
java.lang.IllegalArgumentException: Can not set java.util.List field com.google.api.services.bigquery.model.TableRow.f to java.lang.Double
```

### Original Pipeline Stack Trace
```
    at com.fasterxml.jackson.databind.util.BeanUtil.checkAndFixAccess(BeanUtil.java:187)
    at com.fasterxml.jackson.databind.introspect.AnnotatedField.setValue(AnnotatedField.java:78)
    at com.google.api.client.util.FieldInfo.setValue(FieldInfo.java:129)
    at com.google.api.client.util.GenericData.set(GenericData.java:122)
    at com.google.api.services.bigquery.model.TableRow.set(TableRow.java:1)
```

### Dynamic Destinations Pipeline Stack Trace
```
    at java.base/jdk.internal.reflect.UnsafeFieldAccessorImpl.throwSetIllegalArgumentException(UnsafeFieldAccessorImpl.java:167)
    at java.base/jdk.internal.reflect.UnsafeObjectFieldAccessorImpl.set(UnsafeObjectFieldAccessorImpl.java:81)
    at java.base/java.lang.reflect.Field.set(Field.java:780)
    at com.google.api.client.util.FieldInfo.setFieldValue(FieldInfo.java:281)
    at com.google.api.client.util.GenericData.set(GenericData.java:118)
    at com.google.api.services.bigquery.model.TableRow.set(TableRow.java:64)
```

## Code Analysis

### Original Pipeline Bug Location
The bug occurs in the `ConvertToTableRow.processElement()` method at this line:

```java
TableRow row = new TableRow()
    .set("id", data.id)
    .set("f", data.f)  // ← This line causes the exception
    .set("name", data.name)
    .set("value", data.value);
```

### Dynamic Destinations Pipeline Bug Location
The bug occurs in the `ConvertToTableRowFn.processElement()` method:

```java
TableRow row = new TableRow()
    .set("name", record.name)
    .set("age", record.age)
    .set("f", record.f)  // ← This line causes the exception
    .set("category", record.category);
```

### Root Cause
The issue is that `TableRow` extends `GenericJson` and has a predefined field named `f` of type `List<TableCell>`. When we try to set a Double value to this field, the reflection-based field setting mechanism throws an `IllegalArgumentException`.

### Key Differences Between Pipelines

| Feature | Original Pipeline | Dynamic Destinations Pipeline | Large Batch Pipeline |
|---------|------------------|------------------------------|---------------------|
| BigQuery Write Method | `BigQueryIO.write()` with format functions | `BigQueryIO.writeTableRows()` | `BigQueryIO.writeTableRows()` |
| Destination Type | Static table destination | Dynamic destinations with routing | Dynamic destinations with routing |
| Storage API Method | `STORAGE_WRITE_API` | `STORAGE_API_AT_LEAST_ONCE` | `STORAGE_API_AT_LEAST_ONCE` |
| Optimized Writes | Not used | `.optimizedWrites()` enabled | `.optimizedWrites()` enabled |
| Validation | Default validation | `.withoutValidation()` | `.withoutValidation()` |
| Insert IDs | Default behavior | `.ignoreInsertIds()` | `.ignoreInsertIds()` |
| Table Routing | Single table | Multiple tables based on data category | Multiple tables based on data category |
| Record Size | Small records | Small records | Large records (12MB each) |
| Workaround Support | Field name workaround | No workaround | `withFormatRecordOnFailureFunction` workaround |
| Bug Trigger | Direct field assignment | Direct field assignment | Storage API batch size limit + field conflict |

## Workarounds

1. **Rename the column:** Avoid using 'f' as a column name in your BigQuery schema
2. **Use a different approach:** Consider using Beam's `TableRow` constructor that takes a map of values
3. **Custom serialization:** Implement custom serialization logic that handles the 'f' field specially
4. **Storage API workaround (Large Batch Pipeline):** Use `withFormatRecordOnFailureFunction` to handle the 'f' field conflict during Storage API processing. This workaround is demonstrated in the `LargeBatchBugReproduction` pipeline with the `--enableWorkaround=true` option.

## Cleanup BigQuery Resources

After running the pipelines, you may want to clean up the BigQuery tables that were created. Use the following commands to remove all test tables:

### Clean up Original Pipeline Tables
```bash
# Remove the main test table
bq rm -f manav-jit-test:test.tablerow_bug_test

# Remove the workaround test table
bq rm -f manav-jit-test:test.tablerow_bug_test_workaround
```

### Clean up Dynamic Destinations Pipeline Tables
The dynamic destinations pipeline creates multiple tables based on data categories. To clean them up:

```bash
# List all dynamic destination tables (optional - to see what will be deleted)
bq ls --filter="tableId:dynamic_tablerow_bug_test*" manav-jit-test:test

# Remove all dynamic destination tables
bq rm -f manav-jit-test:test.dynamic_tablerow_bug_test_admins
bq rm -f manav-jit-test:test.dynamic_tablerow_bug_test_default  
bq rm -f manav-jit-test:test.dynamic_tablerow_bug_test_guests
bq rm -f manav-jit-test:test.dynamic_tablerow_bug_test_users
```

### Clean up Large Batch Pipeline Tables
The large batch pipeline creates multiple tables based on data categories. To clean them up:

```bash
# List all large batch tables (optional - to see what will be deleted)
bq ls --filter="tableId:large_batch_bug_test*" manav-jit-test:test

# Remove all large batch tables
bq rm -f manav-jit-test:test.large_batch_bug_test_admins
bq rm -f manav-jit-test:test.large_batch_bug_test_default  
bq rm -f manav-jit-test:test.large_batch_bug_test_guests
bq rm -f manav-jit-test:test.large_batch_bug_test_users

# Remove workaround tables
bq rm -f manav-jit-test:test.large_batch_bug_test_workaround_admins
bq rm -f manav-jit-test:test.large_batch_bug_test_workaround_default  
bq rm -f manav-jit-test:test.large_batch_bug_test_workaround_guests
bq rm -f manav-jit-test:test.large_batch_bug_test_workaround_users
```

### Clean up All Test Tables at Once
To remove all tables created by all three pipelines in one command:

```bash
# Remove all test tables (use with caution!)
for table in tablerow_bug_test tablerow_bug_test_workaround dynamic_tablerow_bug_test_admins dynamic_tablerow_bug_test_default dynamic_tablerow_bug_test_guests dynamic_tablerow_bug_test_users large_batch_bug_test_admins large_batch_bug_test_default large_batch_bug_test_guests large_batch_bug_test_users large_batch_bug_test_workaround_admins large_batch_bug_test_workaround_default large_batch_bug_test_workaround_guests large_batch_bug_test_workaround_users; do
  bq rm -f manav-jit-test:test.$table 2>/dev/null || true
done
```

**Note:** The `-f` flag forces deletion without prompting for confirmation (no y/n prompt). The `2>/dev/null || true` in the loop command ensures the script continues even if a table doesn't exist.

## Technical Details

- **Beam Version:** 2.68.0
- **BigQuery Write Method:** Storage Write API
- **Java Version:** 11
- **Issue Reference:** https://github.com/apache/beam/issues/33531