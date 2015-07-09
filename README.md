This project describes how to run a simple wordcount example using Cascading on QDS.

---

## Compiling the Word Count Jar

### For Hadoop 1
```
$ mvn clean package -Phadoop-1
```

### For Hadoop 2
```
$ mvn clean package -Phadoop-2
```
---

##Running the Word Count Example

This will create an uber jar (i.e. with cascading classes bundled into it) `wordcount-1.0-SNAPSHOT.jar` in the `target` directory. Upload that to S3. Once done, just run a `Hadoop Command` on QDS with following parameters:

```
Job Type: Custom Jar
Path to Jar File: <s3_location>/wordcount-1.0-SNAPSHOT.jar
Arguments: com.qubole.cascading.WordCount s3://paid-qubole/default-datasets/gutenberg/ s3://<output_location>/
```

The final data will be present in `s3://<output_location>/`.

For creating your own Cascading applications on QDS, follow the dependencies as mentioned in the `pom.xml` file.
