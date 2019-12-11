package course;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReverseIndex {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable ONE = new IntWritable(1);
		private Text text = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String fileName = fileSplit.getPath().getName();
			// preprocess file content
			String fileContent = value.toString();
			fileContent.replaceAll("\n", " ");
			fileContent.replaceAll("\r", " ");
			StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
			while (stringTokenizer.hasMoreTokens()) {
				String word = stringTokenizer.nextToken();
				// only write not none word
				word = word.trim();
				if(word.length() > 1) {
					text.set(word + "@@@" + fileName);
					context.write(text, ONE);
				}
			}
		}
	}

	public static class IndexReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	@SuppressWarnings("unchecked")
	public static Map<String, List<String>> collect(String path, Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] files = fs.listStatus(new Path(path));
		BufferedReader br = null;
		Map<String, List<String>> rlt = new HashMap<String, List<String>>();
		Map<String, Map<String, Integer>> data = new HashMap<String, Map<String,Integer>>();
		for (FileStatus file : files) {
			try {
				br = new BufferedReader(new InputStreamReader(fs.open(file.getPath()), "utf-8"));
				String line = br.readLine();
				while (line != null) {
					StringTokenizer st = new StringTokenizer(line);
					String key = st.nextToken();
					String value = st.nextToken();
					String[] tmp = key.split("@@@");
					String word = tmp[0];
					String filename = tmp[1];
					int cnt = Integer.parseInt(value);
					if (data.containsKey(word)) {
						Map<String, Integer> t = data.get(word);
						if (t.containsKey(filename)) {
							cnt = t.get(filename) + cnt;
						}
						t.put(filename, cnt);
						data.put(word, t);
					} else {
						Map<String, Integer> t = new HashMap<String, Integer>();
						t.put(filename, cnt);
						data.put(word, t);
					}
					line = br.readLine();
				}
			} finally {
				if (br != null) {
					br.close();
				}
			}
		}
		// 解析结果文件结束
		// 计算每个文件的单词总数
		Map<String, Integer> fileWordsTotal = new HashMap<String, Integer>();
		for (Map.Entry<String, Map<String, Integer>> entry : data.entrySet()) {
			for (Map.Entry<String, Integer> innerEntry : entry.getValue().entrySet()) {
				String filename = innerEntry.getKey();
				int cnt = innerEntry.getValue();
				if (fileWordsTotal.containsKey(filename)) {
					cnt = fileWordsTotal.get(filename) + cnt;
				}
				fileWordsTotal.put(filename, cnt);
			}
		}
		// 计算每个文件的单词总数结束
		// 计算每个单词出现过的文件
		Map<String, List<String>> wordsFileList = new HashMap<String, List<String>>();
		for (Map.Entry<String, Map<String, Integer>> entry : data.entrySet()) {
			String word = entry.getKey();
			// 计算该词出现过的文件名列表
			List<String> fileList = new ArrayList<String>();
			for (Map.Entry<String, Integer> innerEntry : entry.getValue().entrySet()) {
				String filename = innerEntry.getKey();
				fileList.add(filename);
			}
			// 计算该词出现过的文件名列表结束
			wordsFileList.put(word, fileList);
		}
		// 计算每个单词出现过的文件结束
		// 整理结果
		for (Map.Entry<String, List<String>> entry : wordsFileList.entrySet()) {
			String word = entry.getKey();
			List<String> wordValue = new ArrayList<String>();
			for (String filename : entry.getValue()) {
				int termFreq = data.get(word).get(filename);
				int fileWords = fileWordsTotal.get(filename);
				String value = "(" + filename + "," + termFreq + "," + fileWords + ")";
				wordValue.add(value);
			}
			rlt.put(word, wordValue);
		}
		return rlt;
	}

	public static void main(String[] args) throws Exception {
		System.out.println("main args:");
		for (int i = 0; i < args.length; i++) {
			System.out.println("args " + i + " : " + args[i]);
		}
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ReverseIndex");
		job.setJarByClass(ReverseIndex.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IndexReducer.class);
		job.setReducerClass(IndexReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		for (int i = 0; i < args.length - 1; ++i) {
			System.out.println("Input file " + i + " : " + args[i]);
			FileInputFormat.addInputPath(job, new Path(args[i]));
		}
		System.out.println("OutputPath : " + args[args.length - 1]);
		FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));
		boolean result = job.waitForCompletion(true);
		Map<String, List<String>> rlt = collect(args[args.length - 1], conf);
		for (Map.Entry<String, List<String>> entry : rlt.entrySet()) {
			String word = "";
			int wordCount = entry.getValue().size();
			for (String s : entry.getValue()) {
				word += s + ",";
			}
			System.out.println(entry.getKey() + "->" + wordCount+ "{" + word.substring(0, word.length() - 1) + "}");
		}
		System.exit(result ? 0 : 1);
	}
}