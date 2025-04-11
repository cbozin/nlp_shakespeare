import java.io.IOException;
import java.lang.reflect.Array;
import java.util.StringTokenizer;
import java.util.Collections;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
  
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      ArrayList<String> uniqueWords = new ArrayList<String>();
      ArrayList<Character> punct = new ArrayList<>(Arrays.asList('.', ',', '\'', '"', '!', '?', ';', ':', '-', '_', '(', ')', '}', '{', '[', ']', '+', '/', '='));
      ArrayList<String> stopWords = new ArrayList<String>(Arrays.asList("a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "i", "if", "in", "into", "is", "it", "no", "not", "of", "on", "or", "such",  "that", "the", "their", "then", "there", "these", "they", "this", "to", "was", "will", "with"));
      
      while (itr.hasMoreTokens()) {
        String curr = itr.nextToken().toLowerCase();
        String fixed = "";
        //go through and remove punctuation & stop words
        if(!stopWords.contains(curr)){
          for(int i = 0; i < curr.length(); i++){
            if(!punct.contains(curr.charAt(i))){
              fixed += curr.charAt(i);
            }
          }
          if(!uniqueWords.contains(fixed) && !fixed.isEmpty()){
            uniqueWords.add(fixed);
          }
        }
      }
      Collections.sort(uniqueWords);
      //emit words and pairs of words
      for (int i = 0; i < uniqueWords.size(); ++i) {
          word.set(uniqueWords.get(i));
          context.write(word, one);
          for (int j = i + 1; j < uniqueWords.size(); ++j) {
              word.set(uniqueWords.get(i) + ":" + uniqueWords.get(j));
              context.write(word, one);
          }
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  // public static void main(String[] args) throws Exception {
  //   Configuration conf = new Configuration();
  //   Job job = Job.getInstance(conf, "word count");
  //   job.setJarByClass(WordCount.class);
  //   job.setMapperClass(TokenizerMapper.class);
  //   job.setCombinerClass(IntSumReducer.class);
  //   job.setReducerClass(IntSumReducer.class);
  //   job.setOutputKeyClass(Text.class);
  //   job.setOutputValueClass(IntWritable.class);
  //   FileInputFormat.addInputPath(job, new Path(args[0]));
  //   FileOutputFormat.setOutputPath(job, new Path(args[1]));
  //   System.exit(job.waitForCompletion(true) ? 0 : 1);
  // }
}

