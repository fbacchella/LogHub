package loghub;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.configuration.Properties;

public class Pipeline {

    private static final Logger logger = LogManager.getLogger();

    private final List<PipeStep[]> steps;
    private final String name;
    public final NamedArrayBlockingQueue inQueue;
    public final NamedArrayBlockingQueue outQueue;
    private PipeStep[] wrapper = null;
    private Thread proxy = null;

    public Pipeline(List<PipeStep[]> steps, String name) {
        this.steps = steps;
        this.name = name;
        inQueue = new NamedArrayBlockingQueue(name + ".0");
        outQueue = new NamedArrayBlockingQueue(name + "." + ( steps.size() + 1 ));
        logger.debug("new pipeline from {} to {}", inQueue.name, outQueue.name);
    }

    public boolean configure(Properties properties) {
        return steps.parallelStream().allMatch(i -> i[0].configure(properties));
    }

    public void startStream() {
        logger.debug("{} start stream", name);
        NamedArrayBlockingQueue[] proxies = new NamedArrayBlockingQueue[steps.size() + 1];
        for(int i = 1 ; i < proxies.length - 1 ; i++) {
            proxies[i] = new NamedArrayBlockingQueue(name + "." + i);
        }
        proxies[0] = inQueue;
        proxies[steps.size()] = outQueue;
        int i = 0;
        if(steps.size() > 0) {
            for(PipeStep[] step: steps) {
                for(PipeStep p: step) {
                    p.start(proxies[i], proxies[i+1]);
                }
                i++;
            }
        } else {
            // an empty queue, just forward events
            proxy = Helpers.QueueProxy(name + ".empty", inQueue, outQueue, () -> {logger.error("pipeline {} destination full", name);});
            proxy.start();
        }
    }

    public void stopStream() {
        if(proxy != null) {
            proxy.interrupt();
        } else {
            for(PipeStep[] step: steps) {
                for(PipeStep p: step) {
                    p.interrupt();
                }
            }
        }
    }

    /**
     * Sometime a pipeline must be disguised as a pipestep
     * when used in another pipeline
     * @return
     */
    public PipeStep[] getPipeSteps() {
        if(wrapper == null) {
            wrapper = new PipeStep[] {
                    new PipeStep() {

                        @Override
                        public void start(NamedArrayBlockingQueue endpointIn, NamedArrayBlockingQueue endpointOut) {
                            throw new UnsupportedOperationException("not yet implemented");
                        }

                        @Override
                        public void run() {
                        }

                        @Override
                        public void addProcessor(Processor t) {
                            throw new UnsupportedOperationException("can't add transformer to a pipeline");
                        }

                        @Override
                        public String toString() {
                            return "pipeline(" + Pipeline.this.name + ")";
                        }
                    }
            };
        }

        return wrapper;
    }

    @Override
    public String toString() {
        return "pipeline[" + name + "]." + inQueue.name + "->" + outQueue.name;
    }

}
