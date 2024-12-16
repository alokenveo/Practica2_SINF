package model;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

/**
 * Clase principal para ejecutar múltiples trabajos MapReduce. Procesa datos
 * como frecuencia de URLs, errores HTTP, IPs únicas, frecuencia por hora y
 * distribución por dispositivo.
 */
public class MainMapReduceApp extends Configured implements Tool {

	// CLASES IMPRESCINDIBLES
	/**
	 * Mapper para contar la frecuencia de URLs.
	 */
	public static class FrecuenciaURLMapper extends Mapper<Object, Text, Text, IntWritable> {

		private boolean encabezado = true;

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			if (encabezado) {
				encabezado = false;
				return;
			}

			String[] linea = value.toString().split(",");
			String url = format(linea[2]);
			context.write(new Text(url), new IntWritable(1));
		}
	}

	/**
	 * Reducer para contar la frecuencia de URLs.
	 */
	public static class FrecuenciaURLReducer extends Reducer<Text, IntWritable, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int cont = 0;
			for (IntWritable val : values) {
				cont += val.get();
			}
			context.write(key, new Text(Integer.toString(cont)));
		}
	}

	/**
	 * Mapper para contar los códigos de error HTTP.
	 */
	public static class ErroresHTTPMapper extends Mapper<Object, Text, Text, IntWritable> {

		private boolean encabezado = true;

		@Override
		public void map(Object key, Text text, Context context) throws IOException, InterruptedException {

			if (encabezado) {
				encabezado = false;
				return;
			}

			String[] linea = text.toString().split(",");
			String code = format(linea[3]);

			try {
				int statusCode = Integer.parseInt(code);
				if (statusCode >= 400 && statusCode <= 599) {
					context.write(new Text(code), new IntWritable(1));
				}
			} catch (NumberFormatException e) {
				System.err.println("Código no válido encontrado: " + code);
			}
		}
	}

	/**
	 * Reducer para contar los códigos de error HTTP.
	 */
	public static class ErroresHTTPReducer extends Reducer<Text, IntWritable, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int cont = 0;
			for (IntWritable val : values) {
				cont += val.get();
			}
			context.write(key, new Text(Integer.toString(cont)));
		}
	}

	/**
	 * Mapper para identificar las IPs únicas.
	 */
	public static class IPUnicasMapper extends Mapper<Object, Text, Text, IntWritable> {

		private boolean encabezado = true;

		@Override
		public void map(Object key, Text text, Context context) throws IOException, InterruptedException {

			if (encabezado) {
				encabezado = false;
				return;
			}

			String[] linea = text.toString().split(",");
			String ip = format(linea[1]);
			context.write(new Text(ip), new IntWritable(1));
		}
	}

	/**
	 * Reducer para contar las IPs únicas.
	 */
	public static class IPUnicasReducer extends Reducer<Text, IntWritable, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			HashSet<Object> visitasUnicaSet = new HashSet<>();
			for (@SuppressWarnings("unused")
			IntWritable val : values) {
				visitasUnicaSet.add(key);
			}
			context.write(key, new Text(String.valueOf(visitasUnicaSet.size())));
		}
	}

	/**
	 * Mapper para calcular el número de visitas que ocurrieron en cada hora del
	 * día.
	 */
	public static class FrecuenciaHoraMapper extends Mapper<Object, Text, Text, IntWritable> {

		private boolean encabezado = true;

		@Override
		public void map(Object key, Text text, Context context) throws IOException, InterruptedException {

			if (encabezado) {
				encabezado = false;
				return;
			}

			String[] linea = text.toString().split(",");
			String date = format(linea[0]);
			String[] partesFecha = date.split(":");
			if (partesFecha.length > 1) {
				String hora = partesFecha[1];
				context.write(new Text(hora), new IntWritable(1));
			}
		}
	}

	/**
	 * Reducer para calcular el número de visitas que ocurrieron en cada hora del
	 * día.
	 */
	public static class FrecuenciaHoraReducer extends Reducer<Text, IntWritable, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int cont = 0;
			for (IntWritable val : values) {
				cont += val.get();
			}
			context.write(key, new Text(Integer.toString(cont)));
		}
	}

	/**
	 * Mapper para identificar el porcentaje de accesos realizados desde cada tipo
	 * de dispostivo
	 */
	public static class DistribucionDispositivoMapper extends Mapper<Object, Text, Text, IntWritable> {

		private boolean encabezado = true;

		@Override
		public void map(Object key, Text text, Context context) throws IOException, InterruptedException {

			if (encabezado) {
				encabezado = false;
				return;
			}

			String[] linea = text.toString().split(",");
			String agent = format(linea[4]);
			context.write(new Text(agent), new IntWritable(1));
		}
	}

	/**
	 * Reducer para identificar el porcentaje de accesos realizados desde cada tipo
	 * de dispostivo
	 */
	public static class DistribucionDispositivoReducer extends Reducer<Text, IntWritable, Text, Text> {
		private int visitasTotales = 0;
		private Map<String, Integer> dispositivoVisitasMap = new HashMap<>();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int dispositivoVisitas = 0;

			for (IntWritable val : values) {
				dispositivoVisitas += val.get();
			}

			visitasTotales += dispositivoVisitas;
			dispositivoVisitasMap.put(key.toString(), dispositivoVisitas);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (Map.Entry<String, Integer> entry : dispositivoVisitasMap.entrySet()) {
				String dispositivo = entry.getKey();
				int dispositivoVisitas = entry.getValue();
				double porcentaje = (double) dispositivoVisitas / visitasTotales * 100;
				context.write(new Text(dispositivo), new Text(String.format("%.2f", porcentaje)));
			}
		}
	}

	// CLASES AUXILIARES PRIVADAS
	/**
	 * Elimina el directorio de salida si existe.
	 *
	 * @param args Argumentos de entrada y salida
	 * @throws IOException Si ocurre un error en el sistema de archivos
	 */
	private void deleteOutputFileIfExists(String[] args) throws IOException {
		Path output = new Path(args[1]);
		FileSystem fs = FileSystem.get(getConf());
		if (fs.exists(output)) {
			fs.delete(output, true);
		}
	}

	/**
	 * Formatea un string eliminando espacios en blanco al inicio y final.
	 *
	 * @param value Cadena a formatear
	 * @return Cadena formateada
	 */
	private static String format(String value) {
		return value.trim();
	}

	/**
	 * Configura y ejecuta un trabajo MapReduce.
	 *
	 * @param mapperClass  Clase del Mapper
	 * @param reducerClass Clase del Reducer
	 * @param input        Ruta de entrada
	 * @param output       Ruta de salida
	 * @throws IOException            Si ocurre un error de entrada/salida
	 * @throws ClassNotFoundException Si la clase no es encontrada
	 * @throws InterruptedException   Si el trabajo es interrumpido
	 */
	private void executeJob(Class<? extends Mapper<Object, Text, Text, IntWritable>> mapperClass,
			Class<? extends Reducer<Text, IntWritable, Text, Text>> reducerClass, String input, String output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(getConf(), "Job " + mapperClass.getSimpleName());
		job.setJarByClass(MainMapReduceApp.class);

		job.setMapperClass(mapperClass);
		job.setReducerClass(reducerClass);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.waitForCompletion(true);
	}

	// EJECUCIÓN

	/**
	 * Ejecuta los trabajos MapReduce.
	 *
	 * @param args {Archivo de entrada, directorio de salida}
	 * @return Código de estado
	 * @throws Exception Si ocurre un error durante la ejecución
	 */
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

	/**
	 * Método principal para ejecutar la aplicación.
	 *
	 * @param args Argumentos de entrada y salida
	 * @throws Exception Si ocurre un error durante la ejecución
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new MainMapReduceApp(), args);
	}
}
