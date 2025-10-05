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
│   └── DynamicDestinationTableRowBugReproduction.java # Dynamic destinations pipeline
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

This project contains two different pipeline implementations that reproduce the same TableRow 'f' field bug:

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

| Feature | Original Pipeline | Dynamic Destinations Pipeline |
|---------|------------------|------------------------------|
| BigQuery Write Method | `BigQueryIO.write()` with format functions | `BigQueryIO.writeTableRows()` |
| Destination Type | Static table destination | Dynamic destinations with routing |
| Storage API Method | `STORAGE_WRITE_API` | `STORAGE_API_AT_LEAST_ONCE` |
| Optimized Writes | Not used | `.optimizedWrites()` enabled |
| Validation | Default validation | `.withoutValidation()` |
| Insert IDs | Default behavior | `.ignoreInsertIds()` |
| Table Routing | Single table | Multiple tables based on data category |

## Workarounds

1. **Rename the column:** Avoid using 'f' as a column name in your BigQuery schema
2. **Use a different approach:** Consider using Beam's `TableRow` constructor that takes a map of values
3. **Custom serialization:** Implement custom serialization logic that handles the 'f' field specially

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

### Clean up All Test Tables at Once
To remove all tables created by both pipelines in one command:

```bash
# Remove all test tables (use with caution!)
for table in tablerow_bug_test tablerow_bug_test_workaround dynamic_tablerow_bug_test_admins dynamic_tablerow_bug_test_default dynamic_tablerow_bug_test_guests dynamic_tablerow_bug_test_users; do
  bq rm -f manav-jit-test:test.$table 2>/dev/null || true
done
```

**Note:** The `-f` flag forces deletion without prompting for confirmation (no y/n prompt). The `2>/dev/null || true` in the loop command ensures the script continues even if a table doesn't exist.

## Technical Details

- **Beam Version:** 2.68.0
- **BigQuery Write Method:** Storage Write API
- **Java Version:** 11
- **Issue Reference:** https://github.com/apache/beam/issues/33531