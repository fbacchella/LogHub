package loghub;

import java.util.Collection;
import java.util.List;

import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.openjdk.jmh.util.Statistics;

import boxplot.BoxPlot;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class RunBench {

    public static void main(String[] args) throws Exception {
        System.setProperty("java.awt.headless","true");
        OptionParser parser = new OptionParser("f");
        OptionSpec<String> svgNameOption = parser.accepts("s").withOptionalArg().ofType(String.class);
        OptionSpec<String> testsOption = parser.accepts("t").withOptionalArg().ofType(String.class);
        OptionSpec<String> packagesNameOption = parser.accepts("p").withOptionalArg().ofType(String.class).defaultsTo("queue");
        OptionSet options = parser.parse(args);
        options.nonOptionArguments();

        String packageName = options.valueOf(packagesNameOption);

        long warmupTime;
        int warmupIterations;
        long measurementTime;
        int measurementIterations;
        int forks;
        if (options.has("f")) {
            warmupTime = 1;
            warmupIterations = 1;
            measurementTime = 1;
            measurementIterations = 1;
            forks = 1;
        } else {
            warmupTime = 1;
            warmupIterations = 10;
            // queue std deviation is high for queue, improve precision
            measurementTime = "queue".equals(packageName) ? 30 : 10;
            measurementIterations = 5;
            forks = 5;
        }
        String svgName = options.valueOf(svgNameOption);
        List<String> tests = options.valuesOf(testsOption);
        ChainedOptionsBuilder builder = new OptionsBuilder()
                                                .shouldFailOnError(false)
                                                .warmupIterations(warmupIterations)
                                                .warmupTime(TimeValue.seconds(warmupTime))
                                                .measurementIterations(measurementIterations)
                                                .measurementTime(TimeValue.seconds(measurementTime))
                                                .shouldDoGC(true)
                                                .forks(forks);

        if (tests.isEmpty()) {
            if ("queue".equals(packageName)) {
                builder.include("loghub\\.queue\\.RingBenchmark.+P.+C")
                       .include("loghub\\.queue\\.QueueBenchmark.+P.+C");
            } else if ("datetime".equals(packageName)) {
                builder.include("loghub\\.datetime\\..*ISO8601")
                       .include("loghub\\.datetime\\..*RFC822")
                       .include("loghub\\.datetime\\..*RFC3164");
            } else {
                throw new IllegalArgumentException(packageName);
            }
        } else {
            tests.stream().map(t -> packageName + "." + t).forEach(builder::include);
        }

        Options opt = builder.build();

        Collection<RunResult> results = new Runner(opt).run();

        if (svgName != null) {
            BoxPlot[] plots = new BoxPlot[results.size()];
            int bpRank=0;
            int max = Integer.MIN_VALUE;
            int min = Integer.MAX_VALUE;
            for (RunResult rr: results) {
                System.out.println("******");
                String name = benchName(rr.getParams().getBenchmark());
                System.out.println("    Benchmark: " + rr.getParams().getBenchmark());
                System.out.println("    generatedBenchmark: " + rr.getParams().generatedBenchmark());
                Statistics stats = rr.getPrimaryResult().getStatistics();
                System.out.println("    min; " + stats.getMin());
                System.out.println("    %25; " + stats.getPercentile(25));
                System.out.println("    %50; " + stats.getPercentile(50));
                System.out.println("    %75; " + stats.getPercentile(75));
                System.out.println("    max; " + stats.getMax());

                BoxPlot bp = new BoxPlot(name);
                bp.q1 = stats.getPercentile(25);
                bp.q2 = stats.getPercentile(50);
                bp.q3 = stats.getPercentile(75);
                bp.low = stats.getMin();
                bp.high = stats.getPercentile(95);
                bp.outliers = new double[]{};
                plots[bpRank++] = bp;
                max = Math.max(max, (int) bp.high);
                min = Math.min(min, (int)stats.getMin());
            }
            BoxPlot.generate(svgName, 0, max, max / 5, 0.25, "ns ", "", plots);
        }
    }

    private static String benchName(String benchName) {
        return  benchName.replace("loghub.queue.", "")
                         .replace("loghub.datetime.", "")
                         .replace(".scan", "")
                         .replace(".producing", "");
    }


}
