# Association Rule Confidence Measure with Hadoop MapReduce

This program computes the confidence measure for each association rule derived from a big text file using Hadoop MapReduce. The confidence measure is calculated as 𝐶𝑜𝑛𝑓(𝑋, 𝑌) = 𝑐𝑜𝑢𝑛𝑡(𝑋, 𝑌) / 𝑐𝑜𝑢𝑛𝑡(𝑋), where 𝑐𝑜𝑢𝑛𝑡(𝑋, 𝑌) represents the number of lines that contain both 𝑋 and 𝑌, while 𝑐𝑜𝑢𝑛𝑡(𝑋) is the number of lines that contain 𝑋.

## Features

- Removes punctuation and stop words
- Counts word occurrences and word pairs
- Computes confidence measure for each association rule

## Usage

### Setup

1. Log into ece-hadoop-01.
2. Enter the conf directory.

### Compile

3. Compile the Java program:

```javac -classpath /home/hdoop/hadoop-3.3.1/share/hadoop/common/hadoop-common-3.3.1.jar:/home/hdoop/hadoop-3.3.1/share/hadoop/mapreduce/hadoop-mapreduce-client-core-3.3.1.jar:/home/hdoop/hadoop-3.3.1/share/hadoop/common/lib/commons-cli-1.2.jar -d conf_classes Conf.java```

### Create JAR

4. Create a JAR file:

```jar -cvf conf.jar -C conf_classes/ .```

### Put Files into Input for First Job

5. Put files into the input directory for the first job:

```hadoop fs -put ./input /user/cbozin/conf```

### Run Code

6. Run the program with Hadoop:

```hadoop jar conf.jar Conf /user/cbozin/conf/input /user/cbozin/conf/input1 /user/cbozin/conf/input1 /user/cbozin/conf/output```

### Check Output

7. Check the output:

```hadoop fs -cat /user/cbozin/conf/output/part-r-00000```

To run the program again, make sure to delete the input1 and output directories first.

```hadoop fs -rm -r /user/cbozin/conf/output```

```hadoop fs -rm -r /user/cbozin/conf/input1```
