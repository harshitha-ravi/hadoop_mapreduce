import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/** Class IMDBAnalysis : Contains Mapper class and Reducer class */
public class IMDBAnalysis {

  /** Class TokenizerMapper : inherits the Mapper class, and implements the map function */
  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

    private static final IntWritable one = new IntWritable(1);
    private Text genreYearKey = new Text();

    /**
     * map method() - implements the logic for processing the given data, by reading it line by line
     * and producing the <key,value> pairs as the output, which will be supplied as the input for
     * the reducer</key,value>
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {

      // Fetch the current input split
      // fileSplit - holds the info of current input split
      FileSplit fileSplit = (FileSplit) context.getInputSplit();
      String filename = fileSplit.getPath().getName();

      // Before processing the data, checking whether value is empty
      if (!value.toString().isEmpty()) {

        // First, split the given input string by ";"
        String[] imdbData = value.toString().split(";");

        // Fetch the title type
        String titleType = imdbData[1];

        // Fetch the length of imdbData
        int dataLength = imdbData.length;

        // Fetch the year
        String year = imdbData[3];

        // variable to hold the integer value of year, initially assigning it to 0
        int startYear = 0;

        /* Before parsing the year to integer, check for conditions below:
         * 1 - year != "\N" (Because if "\N" is year value - when tried to parse to int - will cause NumberFormatException)
         * 2 - If imdbData length = 5 : Some movie names contain ";" in their name, hence the split by ";" will create more than 5 splits
         * (which creates problem)
         */
        if (!year.toUpperCase().equals("\\N") && dataLength == 5) {
          startYear = Integer.parseInt(imdbData[3]);
        }

        // Fetch the genre
        String[] genres = imdbData[4].split(",");
        List<String> genreList = Arrays.asList(genres);

        /* Filtering Step :
         * 1 - Check if the titleType = "movie", because only movies are qualified for this requirement
         * 2 - imdb3Data length = 5
         * 3 - year != "\N" */
        if (dataLength == 5
            && titleType.toLowerCase().equals("movie")
            && !year.toUpperCase().equals("\\N")) {

          /* The below if-else block of code - checks for different time periods and within that it checks for the given three combination
           * of genres i.e. "Comedy,Romance", "Action,Drama", "Adventure, Sci-Fi"
           * If the condition satisfies - then <key,value> pair will be written
           * <key,value> format:
           *     key : time-period + genre combination
           *     value : 1
           *
           *     example <key,value> pair : < "[2000-2006],Comedy;Romance," ,  1 >
           * */
          // Time period check : 2000 - 2006
          if (startYear >= 2000 && startYear <= 2006) {

            // Genre check : Comedy, Romance
            if (genreList.contains("Comedy") && genreList.contains("Romance")) {
              String genreKey = "[2000-2006]," + "Comedy;Romance,";
              genreYearKey.set(genreKey);
              context.write(genreYearKey, one);

            }
            // Genre check : Action, Drama
            else if (genreList.contains("Action") && genreList.contains("Drama")) {
              String genreKey = "[2000-2006]," + "Action;Drama,";
              genreYearKey.set(genreKey);
              context.write(genreYearKey, one);

            }
            // Genre check : Adventure, Sci-Fi
            else if (genreList.contains("Adventure") && genreList.contains("Sci-Fi")) {
              String genreKey = "[2000-2006]," + "Adventure;Sci-Fi,";
              genreYearKey.set(genreKey);
              context.write(genreYearKey, one);
            }

          }
          // Time period check : 2007 - 2013
          else if (startYear >= 2007 && startYear <= 2013) {
            // Genre check : Comedy, Romance
            if (genreList.contains("Comedy") && genreList.contains("Romance")) {
              String genreKey = "[2007-2013]," + "Comedy;Romance,";
              genreYearKey.set(genreKey);
              context.write(genreYearKey, one);

            }
            // Genre check : Action, Drama
            else if (genreList.contains("Action") && genreList.contains("Drama")) {
              String genreKey = "[2007-2013]," + "Action;Drama,";
              genreYearKey.set(genreKey);
              context.write(genreYearKey, one);

            }
            // Genre check : Adventure, Sci-Fi
            else if (genreList.contains("Adventure") && genreList.contains("Sci-Fi")) {
              String genreKey = "[2007-2013]," + "Adventure;Sci-Fi,";
              genreYearKey.set(genreKey);
              context.write(genreYearKey, one);
            }

          }
          // Time period check : 2014- 2020
          else if (startYear >= 2014 && startYear <= 2020) {

            // Genre check : Comedy, Romance
            if (genreList.contains("Comedy") && genreList.contains("Romance")) {
              String genreKey = "[2014-2020]," + "Comedy;Romance,";
              genreYearKey.set(genreKey);
              context.write(genreYearKey, one);

            }
            // Genre check : Action, Drama
            else if (genreList.contains("Action") && genreList.contains("Drama")) {
              String genreKey = "[2014-2020]," + "Action;Drama,";
              genreYearKey.set(genreKey);
              context.write(genreYearKey, one);

            }
            // Genre check : Adventure, Sci-Fi
            else if (genreList.contains("Adventure") && genreList.contains("Sci-Fi")) {
              String genreKey = "[2014-2020]," + "Adventure;Sci-Fi,";
              genreYearKey.set(genreKey);
              context.write(genreYearKey, one);
            }
          }
        }
      }
    }
  }

  /** Class IntSumReducer : extends Reducer class and implements reduce function */
  public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable movieCount = new IntWritable();

    /**
     * reduce method : processes the <key,value> pair obtained from the mapper phase - adds up the
     * value to get the final count of the movies for each key (which is the genre combination for
     * each time-period)
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }
      movieCount.set(sum);
      context.write(key, movieCount);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "imdb analysis");
    job.setJarByClass(IMDBAnalysis.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
