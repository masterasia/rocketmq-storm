package com.alibaba.rocketmq.storm.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

/**
 * Created by Administrator on 2015/5/22.
 */
public class BaseMR {
    public static class BaseMapper extends Mapper<Object, Text, Text, Text>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String output = "";
            String line = value.toString();
            if ((line == null) || (line == "") || (line.length() < 27)) {
                return;
            }
            line = PrivateUtils.replaceSeparator(line);

            String jsonLine = line.substring(26);
            BaseBean beans = null;
            try
            {
                ObjectMapper mapper = new ObjectMapper().configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);

                beans = (BaseBean)mapper.readValue(jsonLine, BaseBean.class);
            }
            catch (Exception e)
            {
                return;
            }
            if (beans == null)
            {
                return;
            }
            output = output + beans.getOffId() + "\001";

            output = output + beans.getAffId() + "\001";

            output = output + beans.getClick() + "\001";

            output = output + beans.getConversion() + "\001";

            String logTime = line.substring(1, 20);
            output = output + logTime;

            context.write(null, new Text(output));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.fileoutputformat.compress", "true");
        conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        conf.set("mapreduce.output.fileoutputformat.compress.type", "BLOCK");

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = new Job(conf, "baseMR");
        job.setJarByClass(BaseMR.class);
        job.setMapperClass(BaseMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class PrivateUtils
{
    public static String replaceSeparator(String originalString)
    {
        if (originalString.contains("\001")) {
            originalString.replaceAll("\\\001", "\\\\\\\\001");
        }
        return originalString;
    }
}
