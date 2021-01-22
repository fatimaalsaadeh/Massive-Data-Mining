
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import static java.util.stream.Collectors.toList;

/*
* Fatima AlSaadeh
* Friends Recommendation System
* MapReduce Algorithm Recommends to 10 friends sorted by the number of mutual friends
* The Algorithm :
* Map Procedure:
*   - Loop over all the users list:
*   - Pick the user A:
*   - Loop over the user A friends:
*   - Pair the user A to the already friend users and 0: Key A, Pair (friend1, 0)
*   - Loop over each friend of user A friends and Pair them together with 1
*     which means they have one mutual friend A: Key: friend1, Pair (friend2,1)
*
* The resulted pairs will be shuffled and sorted, combined by the similar keys and sent to the produce
* Produce Procedure:
*   - Get a key with all its pairs
*   - Loop over these pairs:
*   - If the pair with count 0, ignore it because it means the user and suggested are already friends
*   - If the pair with count=1, if it was counted before increase the counter, else add it with count 1
*   - Will end up for key A : (friend1,1), (friend2,4), (friend3,9), where friend1 is suggested and
*     count is how many mutual friends between them
*   - Sort the friends by their mutual friends count and pick the top 10
* */

public class question_1 {

    public static void main(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        Job job = new Job(new Configuration(), "question_1");
        job.setJarByClass(question_1.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(MyPair.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

    public static class Map extends Mapper<LongWritable, Text, IntWritable, MyPair> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String userToFriendsList[] = value.toString().split("\t");

            if (userToFriendsList != null && userToFriendsList.length == 2) {
                Integer user = Integer.parseInt(userToFriendsList[0]);
                List<Integer> friends = Arrays.asList(userToFriendsList[1].split(",")).stream().map(Integer::parseInt).collect(toList());
                for (Integer friend1 : friends) {
                    context.write(new IntWritable(user), new MyPair(friend1, 0));
                    for (Integer friend2: friends) {
                        if(friend1!=friend2) {
                            context.write(new IntWritable(friend1), new MyPair(friend2, 1));
                            context.write(new IntWritable(friend1), new MyPair(friend2, 1));
                        }
                    }
                }
            }
        }
    }


    public static class Reduce extends Reducer<IntWritable, MyPair, Text, Text> {
        @Override
        public void reduce(IntWritable key, Iterable<MyPair> values, Context context)
                throws IOException, InterruptedException {
            Iterator myPairIterator = values.iterator();
            HashMap<Integer, Integer> friendSuggestions = new HashMap<>();

            while (myPairIterator.hasNext()) {
                MyPair mypair = (MyPair) myPairIterator.next();

                if (friendSuggestions.containsKey(mypair.user)) {
                    if (mypair.friend == 0) {
                    } else if (friendSuggestions.get(mypair.user) != 0) {
                        Integer count = friendSuggestions.get(mypair.user);
                        friendSuggestions.put(mypair.user, ++count);
                    }
                } else {
                    if (mypair.friend != 0) {
                        friendSuggestions.put(mypair.user, 1);
                    }
                }
            }
            MyComparator comparator = new MyComparator(friendSuggestions);

            java.util.Map<Integer, Integer> realFriendSuggestions = new TreeMap<>(comparator);
            realFriendSuggestions.putAll(friendSuggestions);

            String[] toPrint = new String[]{"0", "924", "8941", "8942", "9019", "9020", "9021", "9022", "9990", "9992", "9993"};
            if (Arrays.asList(toPrint).contains(key.toString())) {
                String output = StringUtils.join(realFriendSuggestions.keySet().stream().limit(10).toArray(), ",");
                context.write(new Text(key.toString()), new Text(output));
            }


        }
    }

    static public class MyPair implements Writable {

        public Integer user;
        public Integer friend;

        public MyPair(Integer user, Integer friend) {
            this.user = user;
            this.friend = friend;
        }

        public MyPair() {
            this.user = null;
            this.friend = 0;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(user);
            dataOutput.writeInt(friend);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            user = dataInput.readInt();
            friend = dataInput.readInt();

        }
    }

    static class MyComparator implements Comparator<Object> {

        HashMap<Integer, Integer> map;

        public MyComparator(HashMap<Integer, Integer> map) {
            this.map = map;
        }

        public int compare(Object o1, Object o2) {
            Integer o1Int = Integer.parseInt(String.valueOf(o1));
            Integer o2Int = Integer.parseInt(String.valueOf(o2));
            if ((map.get(o1) == map.get(o2) && o1Int < o2Int) || (map.get(o1) > map.get(o2)))
                return -1;
            else
                return 1;

        }
    }

}