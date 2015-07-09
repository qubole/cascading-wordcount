package com.qubole.cascading;

import cascading.flow.*;
import cascading.scheme.hadoop.TextLine;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.*;
import cascading.tap.hadoop.Hfs;
import cascading.tap.*;
import cascading.pipe.*;

import java.io.IOException;
import java.util.*;

import cascading.tuple.Fields;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexGenerator;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {
  public static void main(String[] args) throws IOException {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    System.out.println("Input is " + otherArgs[0] + " Output is " + otherArgs[1]);

    Properties properties = new Properties();

    // define source and sink Taps.
    Scheme sourceScheme = new TextLine(new Fields("line"));
    Tap source = new Hfs(sourceScheme, otherArgs[0]);
    Scheme sinkScheme = new TextLine(new Fields("word", "count"));
    Tap sink = new Hfs(sinkScheme, otherArgs[1], SinkMode.REPLACE);
    // the ‘head’ of the pipe assembly
    Pipe assembly = new Pipe("wordcount");
    // For each input Tuple
    // parse out each word into a new Tuple with the field name “word”
    // regular expressions are optional in Cascading
    String regex = "(?<!\\pL)(?=\\pL)[^ ]*(?<=\\pL)(?!\\pL)";
    Function function = new RegexGenerator(new Fields("word"), regex);
    assembly = new Each(assembly, new Fields("line"), function);
    // group the Tuple stream by the “word” value
    assembly = new GroupBy(assembly, new Fields("word"));
    // For every Tuple group
    // count the number of occurrences of “word” and store result in
    // a field named “count”
    Aggregator count = new Count(new Fields("count"));
    assembly = new Every(assembly, count);
    // initialize app properties, tell Hadoop which jar file to use
    AppProps.setApplicationJarClass(properties, WordCount.class);
    // plan a new Flow from the assembly using the source and sink Taps
    // with the above properties
    FlowConnector flowConnector = new HadoopFlowConnector(properties);
    Flow flow = flowConnector.connect("word-count", source, sink, assembly);
    // execute the flow, block until complete
    flow.complete();
  }
}

