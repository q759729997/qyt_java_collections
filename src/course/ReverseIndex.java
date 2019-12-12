package course;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReverseIndex {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

		private Text keyInfo = new Text();
		private Text valueInfo = new Text("1");

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// input fileContext and fileName
			// output <word@@@fileName@@@fileWordsSize, 1>
			// get fileName
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String fileName = fileSplit.getPath().getName();
			// word segment
			StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
			// word count
			List<String> words = new ArrayList<String>();
			while (stringTokenizer.hasMoreTokens()) {
				String word = stringTokenizer.nextToken();
				// only write not none word
				word = word.trim();
				if (word.length() > 1) {
					words.add(word);
				}
			}
			int fileWordsSize = words.size();
			System.out.println("fileName:" + fileName);
			System.out.println("fileWordsSize:" + fileWordsSize);
			for(String word : words) {
				keyInfo.set(word + "@@@" + fileName + "@@@" + fileWordsSize);
				context.write(keyInfo, valueInfo);
			}
		}
	}
	
	public static class ReverseIndexCombiner extends Reducer<Text, Text, Text, Text>{
		
		private Text keyInfo = new Text();
		private Text valueInfo = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// input <word@@@fileName@@@fileWordsSize, [1, 1, 1, ¡­¡­]>
			// output <word, freq@@@fileName@@@fileWordsSize>
			int freq = 0;
			for (Text val : values) {
				freq += 1;
			}
			String[] keyTokens = key.toString().split("@@@");
			keyInfo.set(keyTokens[0]);
			String fileName = keyTokens[1];
			String fileWordsSize = keyTokens[2];
			valueInfo.set(freq + "@@@" + fileName + "@@@" + fileWordsSize);
			context.write(keyInfo, valueInfo);
		}
	}

	public static class ReverseIndexReducer extends Reducer<Text, Text, Text, Text> {

		private Text keyInfo = new Text();
		private Text valueInfo = new Text("");

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// input <word, [freq@@@fileName@@@fileWordsSize, freq@@@fileName@@@fileWordsSize, ¡­¡­]>
			// output <cat¡ª> 3£º{(d1.txt, 2, 4),(d2.txt,3,5),(d4.txt,1,5)}, "">
			StringBuilder sb = new StringBuilder();
			sb.append(key);
			sb.append(" -> ");
			int freq = 0;
			StringBuilder subSb = new StringBuilder();
			for (Text val : values) {
				freq += 1;
				String[] keyTokens = val.toString().split("@@@");
				subSb.append("(");
				subSb.append(keyTokens[1]); //add fileName
				subSb.append(", ");
				subSb.append(keyTokens[0]); //add freq
				subSb.append(", ");
				subSb.append(keyTokens[2]); //add fileWordsSize
				subSb.append("),");
			}
			sb.append(freq);
			sb.append(" : {");
			sb.append(subSb.toString());
			sb.append("}");
			keyInfo.set(sb.toString());
			System.out.println(keyInfo.toString());
			context.write(keyInfo, valueInfo);
		}
	}


	public static void main(String[] args) throws Exception {
		System.out.println("main args:");
		for (int i = 0; i < args.length; i++) {
			System.out.println("args " + i + " : " + args[i]);
		}
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ReverseIndex");
		job.setJarByClass(ReverseIndex.class);
		// set Mapper class, Combiner class , Reducer class
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(ReverseIndexCombiner.class);
		job.setReducerClass(ReverseIndexReducer.class);
		// set output key and value class
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		for (int i = 0; i < args.length - 1; ++i) {
			System.out.println("Input file " + i + " : " + args[i]);
			FileInputFormat.addInputPath(job, new Path(args[i]));
		}
		System.out.println("OutputPath : " + args[args.length - 1]);
		Path outputPath = new Path(args[args.length - 1]);
		FileSystem fileSystem = FileSystem.get(conf);
		if (fileSystem.exists(outputPath)) {
			fileSystem.delete(outputPath, true);
        }
		FileOutputFormat.setOutputPath(job, outputPath);
		boolean result = job.waitForCompletion(true);
		if (result) {
            System.out.println("job is finished");
        }
		System.exit(result ? 0 : 1);
	}
}