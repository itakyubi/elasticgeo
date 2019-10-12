/*
 * This file is hereby placed into the Public Domain. This means anyone is
 * free to do whatever they wish with this file.
 */
package mil.nga.giat.data.elasticsearch;

import org.elasticsearch.client.Response;
import org.geotools.data.FeatureReader;
import org.geotools.data.store.ContentState;
import org.geotools.util.logging.Logging;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

class ElasticFeatureReaderSlice implements FeatureReader<SimpleFeatureType, SimpleFeature> {

    private final static Logger LOGGER = Logging.getLogger(ElasticFeatureReaderSlice.class);

    private final ContentState contentState;

    private final int maxFeatures;

    private String nextScrollId;

    private ElasticFeatureReader delegate;

    private int numFeatures;

    private boolean lastScroll;

    private final Set<String> scrollIds;

    private List<SimpleFeature> simpleFeatures;

    private List<Response> responses;

    private List<ElasticResponse> searchResponses;

    private int scrollTestCount;

    private int parseTestCount;

    public ElasticFeatureReaderSlice(ContentState contentState, String docType, ElasticRequest elasticRequest, int maxFeatures) {
        this.contentState = contentState;
        this.maxFeatures = maxFeatures;
        this.numFeatures = 0;
        this.scrollIds = new HashSet<>();
        this.simpleFeatures = new ArrayList<SimpleFeature>();
        this.responses = new ArrayList<Response>();
        this.searchResponses = new ArrayList<ElasticResponse>();
        this.scrollTestCount = 0;
        this.parseTestCount = 0;
        LOGGER.fine("ElasticFeatureReaderSlice init +++ <<<");
        try {
            List<Thread> sliceThreads = new ArrayList<>();
            List<ElasticSliceScroll> elasticSliceScrolls = new ArrayList<>();
            LOGGER.fine("slice scroll start!");
            for (int i = 0; i < 5; i++) {
                elasticRequest.setSliceId(i);
                final ElasticDataStore dataStore = (ElasticDataStore) contentState.getEntry().getDataStore();
                ElasticSliceScroll elasticSliceScroll = new ElasticSliceScroll(dataStore, docType, elasticRequest);
                elasticSliceScrolls.add(elasticSliceScroll);
                Thread sliceThread = new Thread(elasticSliceScroll);
                sliceThread.start();
                sliceThreads.add(sliceThread);
            }
            LOGGER.fine("slice scroll start end!");

            LOGGER.fine("slice scroll run!");
            while (!sliceThreads.isEmpty()) {
                Thread t = sliceThreads.get(0);
                sliceThreads.remove(0);
                t.join();
            }
            LOGGER.fine("slice scroll run end!");

            LOGGER.fine("merge scroll response!");
            while (!elasticSliceScrolls.isEmpty()) {
                ElasticSliceScroll ElasticSliceScroll = elasticSliceScrolls.get(0);
                elasticSliceScrolls.remove(0);
                List<Response> sliceResponses = ElasticSliceScroll.getResponses();
                responses.addAll(sliceResponses);
            }
            LOGGER.fine("merge scroll response end!");

            scrollInit();
        } catch (IOException | InterruptedException e) {
            LOGGER.fine("ElasticFeatureReaderSlice Exception e:" + e);
            e.printStackTrace();
        }
        LOGGER.fine("ElasticFeatureReaderScroll init --- >>>");
    }

    private void scrollInit() throws IOException, InterruptedException {
        LOGGER.fine("processer.init +++");
        List<Thread> ts = new ArrayList<Thread>();
        List<ReponseToElasticResponse> processers = new ArrayList<ReponseToElasticResponse>();
        while (!responses.isEmpty()) {
            Response response = responses.get(0);
            responses.remove(0);
            ReponseToElasticResponse processer = new ReponseToElasticResponse(response);
            processers.add(processer);
            Thread t = new Thread(processer);
            t.start();
            ts.add(t);
        }
        LOGGER.fine("processer.init ---");

        // processing
        LOGGER.fine("processer.run +++");
        while (!ts.isEmpty()) {
            Thread t = ts.get(0);
            ts.remove(0);
            t.join();
            LOGGER.fine("t.join()");
        }
        LOGGER.fine("processer.run ---");

        LOGGER.fine("parseTest.init +++");
        while (!processers.isEmpty()) {
            ReponseToElasticResponse processer = processers.get(0);
            processers.remove(0);
            ElasticResponse searchResponse = processer.getElasticResponse();
//            ElasticResponse searchResponse = dataStore.getClient().parseTest(response);
            searchResponses.add(searchResponse);
        }
        LOGGER.fine("parseTest.init ---");

        LOGGER.fine("parseResponseTest +++");
        parseResponseTest();
        LOGGER.fine("parseResponseTest ---");
        LOGGER.fine("parseResponseTest numFeatures:" + this.numFeatures);
        LOGGER.fine("parseResponseTest maxFeatures:" + this.maxFeatures);
        LOGGER.fine("parseResponseTest simpleFeatures size:" + simpleFeatures.size());
    }

    private void parseResponseTest() throws IOException {
        while (!searchResponses.isEmpty()) {
            ElasticResponse searchResponse = searchResponses.get(0);
            searchResponses.remove(0);

            //private void processResponse(ElasticResponse searchResponse)
            {
                final int numHits = searchResponse.getNumHits();
                final List<ElasticHit> hits;
                if (numFeatures + numHits <= maxFeatures) {
                    hits = searchResponse.getResults().getHits();
                } else {
                    final int n = maxFeatures - numFeatures;
                    hits = searchResponse.getResults().getHits().subList(0, n);
                }
                delegate = new ElasticFeatureReader(contentState, hits, searchResponse.getAggregations(), 0);
                nextScrollId = searchResponse.getScrollId();
                lastScroll = numHits == 0 || numFeatures + hits.size() >= maxFeatures;
                LOGGER.fine("Scoll numHits=" + hits.size() + " (total=" + (numFeatures + hits.size()));
                scrollIds.add(nextScrollId);
            }
            //private SimpleFeature nextInternal() throws IOException
            while (delegate.hasNext()) {
                numFeatures++;
                simpleFeatures.add(delegate.next());
            }
        }
    }

    private void processResponse(ElasticResponse searchResponse) {
        final int numHits = searchResponse.getNumHits();
        final List<ElasticHit> hits;
        if (numFeatures + numHits <= maxFeatures) {
            hits = searchResponse.getResults().getHits();
        } else {
            final int n = maxFeatures - numFeatures;
            hits = searchResponse.getResults().getHits().subList(0, n);
        }
        delegate = new ElasticFeatureReader(contentState, hits, searchResponse.getAggregations(), 0);
        nextScrollId = searchResponse.getScrollId();
        lastScroll = numHits == 0 || numFeatures + hits.size() >= maxFeatures;
        LOGGER.fine("Scoll numHits=" + hits.size() + " (total=" + (numFeatures + hits.size()));
        scrollIds.add(nextScrollId);
    }

    @Override
    public SimpleFeatureType getFeatureType() {
        return delegate.getFeatureType();
    }

    @Override
    public SimpleFeature next() throws IOException {
        final SimpleFeature feature;
        if (hasNext()) {
            feature = simpleFeatures.get(0);
            simpleFeatures.remove(0);
        } else {
            throw new NoSuchElementException();
        }
        return feature;
    }

    @Override
    public boolean hasNext() throws IOException {
        return !simpleFeatures.isEmpty();
    }


    @Override
    public void close() throws IOException {
        if (!scrollIds.isEmpty()) {
            final ElasticDataStore dataStore;
            dataStore = (ElasticDataStore) contentState.getEntry().getDataStore();
            dataStore.getClient().clearScroll(scrollIds);
        }
        delegate.close();
    }
}
