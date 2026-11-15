import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EcommerceMapReduce {

    // ==============================
    // CSV PARSER
    // ==============================

    public static List<String> parseCSV(String line) {

        List<String> fields = new ArrayList<>();
        StringBuilder field = new StringBuilder();

        boolean insideQuotes = false;

        for (int i = 0; i < line.length(); i++) {

            char c = line.charAt(i);

            if (c == '"') {
                insideQuotes = !insideQuotes;
            }
            else if (c == ',' && !insideQuotes) {
                fields.add(field.toString().trim());
                field.setLength(0);
            }
            else {
                field.append(c);
            }
        }

        fields.add(field.toString().trim());

        return fields;
    }


    // ==========================================================
    // 3.1 COUNT RECORDS BY CATEGORY
    // ==========================================================

    public static class CategoryMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private static final IntWritable ONE =
                new IntWritable(1);

        private Text category = new Text();

        @Override
        public void map(
                Object key,
                Text value,
                Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // Skip CSV header
            if (line.startsWith("order_id,")) {
                return;
            }

            List<String> fields = parseCSV(line);

            // Current dataset has 18 columns
            if (fields.size() < 18) {
                return;
            }

            // category = column 3
            String categoryValue = fields.get(3);

            if (!categoryValue.isEmpty()) {

                category.set(categoryValue);

                context.write(category, ONE);
            }
        }
    }


    public static class CategoryReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result =
                new IntWritable();

        @Override
        public void reduce(
                Text key,
                Iterable<IntWritable> values,
                Context context)
                throws IOException, InterruptedException {

            int total = 0;

            for (IntWritable value : values) {
                total += value.get();
            }

            result.set(total);

            context.write(key, result);
        }
    }


    // ==========================================================
    // 3.2 TOTAL SALES BY CATEGORY
    // ==========================================================

    public static class SalesMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        private Text category = new Text();

        private DoubleWritable sales =
                new DoubleWritable();

        @Override
        public void map(
                Object key,
                Text value,
                Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // Skip header
            if (line.startsWith("order_id,")) {
                return;
            }

            List<String> fields = parseCSV(line);

            // Current dataset has 18 columns
            if (fields.size() < 18) {
                return;
            }

            // category = column 3
            String categoryValue = fields.get(3);

            try {

                // unit_price_usd = column 5
                double price =
                        Double.parseDouble(fields.get(5));

                // quantity = column 4
                double quantity =
                        Double.parseDouble(fields.get(4));

                // Quantity × Unit Price
                double totalSales =
                        quantity * price;

                if (!categoryValue.isEmpty()) {

                    category.set(categoryValue);

                    sales.set(totalSales);

                    context.write(category, sales);
                }

            } catch (NumberFormatException e) {

                // Ignore invalid numerical records
            }
        }
    }


    public static class SalesReducer
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable result =
                new DoubleWritable();

        @Override
        public void reduce(
                Text key,
                Iterable<DoubleWritable> values,
                Context context)
                throws IOException, InterruptedException {

            double totalSales = 0.0;

            for (DoubleWritable value : values) {
                totalSales += value.get();
            }

            result.set(totalSales);

            context.write(key, result);
        }
    }


    // ==========================================================
    // 3.3 COUNT RECORDS BY PAYMENT METHOD
    // ==========================================================

    public static class PaymentMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private static final IntWritable ONE =
                new IntWritable(1);

        private Text paymentMethod = new Text();

        @Override
        public void map(
                Object key,
                Text value,
                Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // Skip CSV header
            if (line.startsWith("order_id,")) {
                return;
            }

            List<String> fields = parseCSV(line);

            // Current dataset has 18 columns
            if (fields.size() < 18) {
                return;
            }

            // payment_method = column 7
            String payment =
                    fields.get(7);

            if (!payment.isEmpty()) {

                paymentMethod.set(payment);

                context.write(paymentMethod, ONE);
            }
        }
    }


    public static class PaymentReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result =
                new IntWritable();

        @Override
        public void reduce(
                Text key,
                Iterable<IntWritable> values,
                Context context)
                throws IOException, InterruptedException {

            int total = 0;

            for (IntWritable value : values) {
                total += value.get();
            }

            result.set(total);

            context.write(key, result);
        }
    }


    // ==========================================================
    // MAIN METHOD
    // ==========================================================

    public static void main(String[] args)
            throws Exception {

        if (args.length != 3) {

            System.err.println(
                    "Usage: EcommerceMapReduce <mode> <input> <output>");

            System.exit(1);
        }

        String mode = args[0];

        String input = args[1];

        String output = args[2];

        Configuration conf =
                new Configuration();

        Job job =
                Job.getInstance(
                        conf,
                        "E-Commerce MapReduce");

        job.setJarByClass(
                EcommerceMapReduce.class);

        FileInputFormat.addInputPath(
                job,
                new Path(input));

        FileOutputFormat.setOutputPath(
                job,
                new Path(output));


        // --------------------------------
        // CATEGORY COUNT
        // --------------------------------

        if (mode.equals("category_count")) {

            job.setMapperClass(
                    CategoryMapper.class);

            job.setReducerClass(
                    CategoryReducer.class);

            job.setOutputKeyClass(
                    Text.class);

            job.setOutputValueClass(
                    IntWritable.class);
        }


        // --------------------------------
        // SALES
        // --------------------------------

        else if (mode.equals("sales")) {

            job.setMapperClass(
                    SalesMapper.class);

            job.setReducerClass(
                    SalesReducer.class);

            job.setOutputKeyClass(
                    Text.class);

            job.setOutputValueClass(
                    DoubleWritable.class);
        }


        // --------------------------------
        // PAYMENT COUNT
        // --------------------------------

        else if (mode.equals("payment_count")) {

            job.setMapperClass(
                    PaymentMapper.class);

            job.setReducerClass(
                    PaymentReducer.class);

            job.setOutputKeyClass(
                    Text.class);

            job.setOutputValueClass(
                    IntWritable.class);
        }


        else {

            System.err.println(
                    "Unknown mode: " + mode);

            System.exit(1);
        }


        System.exit(
                job.waitForCompletion(true)
                        ? 0
                        : 1);
    }
}