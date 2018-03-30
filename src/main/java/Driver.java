import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Driver {

    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
        //job1
        Configuration conf1 = new Configuration();

        //Define the job to read data sentence by sentence
        conf1.set("textinputformat.record.delimiter", ".");
        conf1.set("noGram", args[2]);

        Job job1 = Job.getInstance();
        job1.setJobName("NGram");
        job1.setJarByClass(Driver.class);

        job1.setMapperClass(NGramLibraryBuilder.NGramMapper.class);
        job1.setReducerClass(NGramLibraryBuilder.NGramReducer.class);

        //Both the output key of map and reduce are Text type
        //Both the output value of map and reduce are IntWritable type
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(job1, new Path(args[0]));
        TextOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);

        //how to connect two jobs?
        // last output is second input

        //2nd job
        Configuration conf2 = new Configuration();
        conf2.set("threshold", args[3]);
        conf2.set("topK", args[4]);

        //Use dbConfiguration to configure all the jdbcDriver, db user, db password, dataBase
        DBConfiguration.configureDB(conf2,
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://192.168.0.15:8889/test",
                "root",
                "root");

        Job job2 = Job.getInstance(conf2);
        job2.setJobName("Model");
        job2.setJarByClass(Driver.class);


        //How to add external dependency to current project?
        /**
         * 1. upload dependency to hdfs
         * 2. use this "addArchiveToClassPath" method to define the dependency path on hdfs
        */
        job2.addArchiveToClassPath(new Path("path_to_ur_connector"));
        //Why do we add map outputKey and map outputValue?
        //Because map output key and value are inconsistent with reducer output key and value
        //set output key type and output value type for map
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        //set output key type and output value type for reduce
        job2.setOutputKeyClass(DBOutputWritable.class);
        job2.setOutputValueClass(NullWritable.class);

        job2.setMapperClass(LanguageModel.Map.class);
        job2.setReducerClass(LanguageModel.Reduce.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(DBOutputFormat.class);

        //use dbOutputFormat to define the table name and columns
        DBOutputFormat.setOutput(job2, "output",
                new String[] {"starting_phrase", "following_word", "count"});

        TextInputFormat.setInputPaths(job2, args[1]);
        job2.waitForCompletion(true);
    }

}
