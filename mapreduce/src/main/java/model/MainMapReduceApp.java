package model;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MainMapReduceApp extends Configured implements Tool {

	// CLASES IMPRESCINDIBLES
	public static class FrecuenciaURLMapper extends Mapper<Object, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] linea = value.toString().split(",");
			String url = format(linea[2]);
			context.write(new Text(url), new IntWritable(1));
		}
	}

	public static class FrecuenciaURLReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int cont = 0;
			for (IntWritable val : values) {
				cont += val.get();
			}
			context.write(key, new IntWritable(cont));
		}
	}

	public static class ErroresHTTPMapper extends Mapper<Object, Text, Text, IntWritable> {
		public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
			String[] linea = text.toString().split(",");
			String code = format(linea[3]);
			context.write(new Text(code), new IntWritable(1));
		}
	}

	public static class ErroresHTTPReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int cont = 0;
			for (IntWritable val : values) {
				cont += val.get();
			}
			context.write(key, new IntWritable(cont));
		}
	}

	public static class IPUnicasMapper extends Mapper<Object, Text, Text, IntWritable> {
		public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
			String[] linea = text.toString().split(",");
			String ip = format(linea[3]);
			context.write(new Text(ip), new IntWritable(1));
		}
	}

	public static class IPUnicasReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			HashSet<Object> visitasUnicaSet = new HashSet<>();
			for (IntWritable val : values) {
				visitasUnicaSet.add(key);
			}
			context.write(key, new IntWritable(visitasUnicaSet.size()));
		}
	}

	public static class FrecuenciaHoraMapper extends Mapper<Object, Text, Text, IntWritable> {
		public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
			String[] linea = text.toString().split(",");
			String date = format(linea[0]);
			String hora = date.split(":")[1];
			context.write(new Text(hora), new IntWritable(1));
		}
	}

	public static class FrecuenciaHoraReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable val : values) {
				count += val.get();
			}
			context.write(key, new IntWritable(count));
		}
	}

	public static class DistribucionDispositivoMapper extends Mapper<Object, Text, Text, IntWritable> {
		public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
			String[] linea = text.toString().split(",");
			String agent = format(linea[4]);
			context.write(new Text(agent), new IntWritable(1));
		}
	}

	public static class DistribucionDispositivoReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int visitasTotales = 0;
			int dispositivoVisitas = 0;
			for (IntWritable val : values) {
				visitasTotales++;
				dispositivoVisitas += val.get();
			}

			double porcentaje = ((double) dispositivoVisitas / visitasTotales) * 100;
			context.write(key, new DoubleWritable(porcentaje));
		}
	}

	// CLASES AUXILIARES PRIVADAS
	private void deleteOutputFileIfExists(String[] args) throws IOException {
		Path output = new Path(args[1]);
		FileSystem fs = FileSystem.get(getConf());
		if (fs.exists(output)) {
			fs.delete(output, true);
		}
	}

	private static String format(String value) {
		return value.trim();
	}

	private void executeJob(Class<? extends Mapper> mapperClass, Class<? extends Reducer> reducerClass, String input,
			String output) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(getConf(), "Job " + mapperClass.getSimpleName());
		job.setJarByClass(MainMapReduceApp.class);

		job.setMapperClass(mapperClass);
		job.setReducerClass(reducerClass);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.waitForCompletion(true);
	}

	// EJECUCIÓN

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Faltan parámetros requeridos: {input file} {output file}");
			System.exit(-1);
		}

		deleteOutputFileIfExists(args);

		executeJob(FrecuenciaURLMapper.class, FrecuenciaURLReducer.class, args[0], args[1] + "/visitas_por_url");

		executeJob(ErroresHTTPMapper.class, ErroresHTTPReducer.class, args[0], args[1] + "/errores_http");

		executeJob(IPUnicasMapper.class, IPUnicasReducer.class, args[0], args[1] + "/visitas_por_ip_unica");

		executeJob(FrecuenciaHoraMapper.class, FrecuenciaHoraReducer.class, args[0], args[1] + "/frecuencia_por_hora");

		executeJob(DistribucionDispositivoMapper.class, DistribucionDispositivoReducer.class, args[0],
				args[1] + "/distribucion_por_dispositivo");

		return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new MainMapReduceApp(), args);
	}
}
