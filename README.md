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
├── pom.xml                                    # Maven configuration with Beam dependencies
├── src/main/java/com/example/
│   └── BQTableRowBugReproduction.java        # Main pipeline class
└── README.md                                 # This file
```

## Prerequisites

1. Java 11 or higher
2. Maven 3.6 or higher
3. Google Cloud Platform account with BigQuery enabled
4. GCP credentials configured (via `gcloud auth application-default login` or service account key)

## Setup Instructions

1. **Clone or navigate to this directory:**
   ```bash
   cd /Users/xqhu/Trae/df-bq-tests
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

### Test the Bug (Default Behavior)
To reproduce the TableRow 'f' column bug, run:

```bash
mvn compile exec:java \
  -Dexec.mainClass=com.example.BQTableRowBugReproduction \
  -Dexec.args="--project=manav-jit-test \
               --dataset=test \
               --table=tablerow_bug_test \
               --runner=DirectRunner"
```

### Test with Dataflow Runner
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

### Test with Workaround Enabled
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

### Test Workaround with Dataflow Runner
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

## Expected Error

When you run this pipeline, you should see the following error:

```
java.lang.IllegalArgumentException: Can not set java.util.List field com.google.api.services.bigquery.model.TableRow.f to java.lang.Double
    at com.fasterxml.jackson.databind.util.BeanUtil.checkAndFixAccess(BeanUtil.java:187)
    at com.fasterxml.jackson.databind.introspect.AnnotatedField.setValue(AnnotatedField.java:78)
    at com.google.api.client.util.FieldInfo.setValue(FieldInfo.java:129)
    at com.google.api.client.util.GenericData.set(GenericData.java:122)
    at com.google.api.services.bigquery.model.TableRow.set(TableRow.java:1)
```

## Code Analysis

The bug occurs in the `ConvertToTableRow.processElement()` method at this line:

```java
TableRow row = new TableRow()
    .set("id", data.id)
    .set("f", data.f)  // ← This line causes the exception
    .set("name", data.name)
    .set("value", data.value);
```

The issue is that `TableRow` extends `GenericJson` and has a predefined field named `f` of type `List<TableCell>`. When we try to set a Double value to this field, Jackson throws an `IllegalArgumentException`.

## Workarounds

1. **Rename the column:** Avoid using 'f' as a column name in your BigQuery schema
2. **Use a different approach:** Consider using Beam's `TableRow` constructor that takes a map of values
3. **Custom serialization:** Implement custom serialization logic that handles the 'f' field specially

## Technical Details

- **Beam Version:** 2.68.0
- **BigQuery Write Method:** Storage Write API
- **Java Version:** 11
- **Issue Reference:** https://github.com/apache/beam/issues/33531