import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.jfree.chart.*;
import org.jfree.chart.plot.PlotOrientation;
import javax.swing.*;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import java.io.*;
import java.util.*;

/*
* Fatima AlSaadeh
* Iterative Kmeans
* Pass arg[0] on c1.txt, c2.txt for changing the centroid file.
* */

public class question_4 extends Configured {
    public static ArrayList<ArrayList<Double>> clusters;
    public static ArrayList<Double> lastCost = new ArrayList<>();
    public static HashMap<Integer, ArrayList<Double>> costIterations = new HashMap<>();
    public static int d = 58;

    public static void main(String[] args) throws Exception {
        int iterations = 0;
        String clustersFile = args[0];
        String outputFolder = "output2/";
        String prevResultsFile = "/part-r-00000";
        String dataFile = "src/main/resources/data/data.txt";
        String prevResultsFolder = "output2/0";
        Configuration conf = new Configuration();
        readFile(clustersFile);
        conf.set("iteration", String.valueOf(iterations));
        Job job = new Job(conf, "question_4");
        job.setJarByClass(question_4.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(dataFile));
        FileOutputFormat.setOutputPath(job, new Path(outputFolder + "0"));
        job.waitForCompletion(true);
        iterations++;
        while (iterations < 20) {
            conf = new Configuration();
            conf.set("iteration", String.valueOf(iterations));
            clustersFile = prevResultsFolder + prevResultsFile;
            readFile(clustersFile);
            job = new Job(conf, "question_4");
            job.setJarByClass(question_4.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setMapperClass(Map.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setReducerClass(Reduce.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.addInputPath(job, new Path(dataFile));
            FileOutputFormat.setOutputPath(job, new Path(outputFolder + "" + iterations));
            job.waitForCompletion(true);
            prevResultsFolder = outputFolder + "" + iterations;
            iterations++;
        }
        for (int i = 0; i < costIterations.size(); i++) {
            ArrayList<Double> a;
            a = costIterations.get(i);
            double sum = 0.0;
            for (int j = 0; j < a.size(); j++) {
                sum += a.get(j);
            }
            System.out.println(sum);
            lastCost.add(sum);
        }
        System.out.println("percentage change in cost after 10 iterations");
        System.out.println(((lastCost.get(0)-lastCost.get(9))/lastCost.get(0))*100);
        draw.main();
    }
    public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
        private ArrayList<Double> points = new ArrayList<>();
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            for (String point : value.toString().split("\\s")) {
                points.add(Double.parseDouble(point));
            }
            double distance = 0.0;
            int centroied = 0;
            for (int i = 0; i < clusters.size(); i++) {
                double sum = costFunction(points, clusters.get(i));
                if (i == 0) {
                    distance = sum;
                } else if (sum < distance) {
                    centroied = i;
                    distance = sum;

                }
            }

            int iteration = Integer.parseInt(context.getConfiguration().get("iteration"));
            if (costIterations.get(iteration) == null) {
                ArrayList<Double> a = new ArrayList<>();
                a.add(distance * distance);
                costIterations.put(iteration, a);

            } else {
                ArrayList<Double> a = costIterations.get(iteration);
                a.add(distance * distance);
                costIterations.put(iteration, a);
            }

            context.write(new IntWritable(centroied), value);
            points.clear();
        }

    }

    public static class Reduce extends Reducer<IntWritable, Text, Text, Text> {
        private ArrayList<Double> points;
        @Override
        public void reduce(IntWritable key, Iterable<Text> value, Context context)
                throws IOException, InterruptedException {
            Double sum[] = new Double[d];
            Arrays.fill(sum, 0.0);
            int num = 0;
            for (Text val : value) {
                int i = 0;
                num++;
                points = new ArrayList<>();
                for (String point : val.toString().split("\\s+")) {
                    points.add(Double.parseDouble(point));
                    sum[i] += Double.parseDouble(point);
                    i++;
                }
            }
            String cluster = "";
            for (int i = 0; i < d; i++) {
                cluster += String.valueOf(sum[i] / num);
                cluster += " ";
            }
            context.write(new Text(cluster),new Text(""));
        }
    }

    public static void readFile(String filePath) {
        ArrayList<Double> arrDouble;
        clusters = new ArrayList<>();
        try {
            Scanner scanner = new Scanner(new File(filePath));
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                String[] list = line.split("\\s");
                arrDouble = new ArrayList<>();
                for (String s : list) {
                    arrDouble.add(Double.parseDouble(s));
                }
                clusters.add(arrDouble);
            }
            scanner.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static double costFunction(ArrayList<Double> a, ArrayList<Double> b) {
        double sum = 0.0;
        for (int i = 0; i < a.size(); i++) {
            sum += Math.pow(a.get(i) - b.get(i), 2);
        }
        return Math.sqrt(sum);
    }
    public static class draw extends JFrame {
        public draw() {
            XYDataset dataset = createDataset();
            JFreeChart chart = ChartFactory.createXYLineChart(
                    "C2.txt Cost Chart",
                    "Iterations",
                    "Cost",
                    dataset,
                    PlotOrientation.VERTICAL,
                    true, true, false);
            ChartPanel panel = new ChartPanel(chart);
            setContentPane(panel);
        }
        private XYDataset createDataset() {
            XYSeriesCollection dataset = new XYSeriesCollection();
            XYSeries series = new XYSeries("Cost");
            for(int i= 0; i< lastCost.size();i++) {
                series.add(i, lastCost.get(i));
            }
            dataset.addSeries(series);

            return dataset;
        }
        public static void main() {
            SwingUtilities.invokeLater(() -> {
                draw costFunctionPlot = new draw();
                costFunctionPlot.setSize(700, 600);
                costFunctionPlot.setVisible(true);
                costFunctionPlot.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
            });
        }

    }
}

