import java.io.*;
import java.util.*;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

public class Conf
{
    // Mapper class for counting word occurrences and word pairs
    static class TokenizerMapper
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
        //sort list of words
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
	// Reducer class for summing up word counts
  static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
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

	// Mapper class for processing word count output
	public static class AssociationMapper extends Mapper<Object, Text, Text, Text>
	{
		// Text object to store the key
		private Text outputKey = new Text();
		// Text object to store the value
		private Text outputValue = new Text();

		// Map function: processes word count output, reformatting it for the ConfidenceReducer
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			StringTokenizer lineItr = new StringTokenizer(value.toString(), "\n");
			while (lineItr.hasMoreTokens())
			{
				String line = lineItr.nextToken();
				StringTokenizer itr = new StringTokenizer(line);
				String word = itr.nextToken();
				String count = itr.nextToken();

				if (word.contains(":"))
				{
					itr = new StringTokenizer(word, ":");
					String first = itr.nextToken();
					String second = itr.nextToken();

					outputKey.set(first);
					outputValue.set(second + "|" + count);
					context.write(outputKey, outputValue); // x, y|count(x,y)

					outputKey.set(second);
					outputValue.set(first + "|" + count);
					context.write(outputKey, outputValue); // y, x|count(x,y)
				}
				else
				{
					outputKey.set(word);
					outputValue.set(count);
					context.write(outputKey, outputValue); // x, count(x)
				}
			}
		}
	}
	// Reducer class for calculating association confidence scores
	public static class ConfidenceReducer extends Reducer<Text, Text, Text, Text>
	{
		// Text object to store the key
		private Text outputKey = new Text();
		
		// Text object to store the value
		private Text outputValue = new Text();

		// Reduce function: calculates and emits confidence scores for word associations
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			String valueText;
			int cnt = 0;

			// Find count(x)
			for (Text val : values)
			{
				valueText = val.toString();
				if (!valueText.contains("|"))
				{
					cnt = Integer.parseInt(valueText);
					break;
				}
			}

			// Emit conf(x,y) = count(x,y)/count(x)
			for (Text val : values)
			{
				valueText = val.toString();
				if (valueText.contains("|"))
				{
					StringTokenizer itr = new StringTokenizer(valueText, "|");
					String word = itr.nextToken();
					double num = (double) Integer.parseInt(itr.nextToken());

					outputKey.set(key.toString() + ":" + word);
					num /= count;
					outputValue.set("" + num);
					context.write(outputKey, outputValue); // x:y, count(x,y)/count(x)
				}
			}
		}
	}

	// Main function: sets up and runs two MapReduce jobs (word count and association confidence scoring)
	public static void main(String[] args) throws Exception
	{
		// Create a Configuration object for the MapReduce jobs
		Configuration conf = new Configuration();
		GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
		String[] remainingArgs = optionParser.getRemainingArgs();
		List<String> otherArgs = new ArrayList<>();

		// Set up the first MapReduce job for word count
		Job job = Job.getInstance(conf, "Conf step one");
		job.setJarByClass(Conf.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Parse optional args
		for (int i = 0; i < remainingArgs.length; ++i)
		{
			if ("-skip".equals(remainingArgs[i]))
			{
				job.addCacheFile(new Path(remainingArgs[++i]).toUri());
				job.getConfiguration().setBoolean("conf.skip.patterns", true);
				job.getConfiguration().setBoolean("conf.case.sensitive", false);
			}
			else
				otherArgs.add(remainingArgs[i]);
		}

		FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));
		job.waitForCompletion(true);

		// Set up the second MapReduce job for association confidence scoring
		Configuration confTwo = new Configuration();
		Job job2 = Job.getInstance(confTwo, "Conf step two");
		job2.setJarByClass(Conf.class);
		job2.setMapperClass(AssociationMapper.class);
		job2.setReducerClass(ConfidenceReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job2, new Path(otherArgs.get(2)));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs.get(3)));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}