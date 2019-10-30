/*
 * This file is hereby placed into the Public Domain. This means anyone is
 * free to do whatever they wish with this file.
 */
package mil.nga.giat.data.elasticsearch;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

import org.geotools.data.FeatureReader;
import org.geotools.data.store.ContentState;
import org.geotools.util.logging.Logging;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

class ElasticFeatureReaderScroll implements FeatureReader<SimpleFeatureType, SimpleFeature> {

    private final static Logger LOGGER = Logging.getLogger(ElasticFeatureReaderScroll.class);

    private final ContentState contentState;

    private final int maxFeatures;

    private String nextScrollId;

    private ElasticFeatureReader delegate;

    private int numFeatures;

    private boolean lastScroll;

    private final Set<String> scrollIds;

    private final List<SimpleFeature> simpleFeatures;

    private String docType;

    public ElasticFeatureReaderScroll(ContentState contentState, ElasticResponse searchResponse, int maxFeatures,List<SimpleFeature> cachedSimpleFeatures,String docType) throws IOException {
        this.contentState = contentState;
        this.maxFeatures = maxFeatures;
        this.numFeatures = 0;
        this.scrollIds = new HashSet<>();
        this.docType = docType;
        processResponse(searchResponse);

        // fill simpleFeatures
        this.simpleFeatures = new ArrayList<SimpleFeature>();
        if((cachedSimpleFeatures != null) && (!cachedSimpleFeatures.isEmpty())) {
            simpleFeatures.addAll(cachedSimpleFeatures);
        }
        while(hasNextInternal()) {
            simpleFeatures.add(nextInternal());
        }
    }

    private void advanceScroll() throws IOException {
        final ElasticDataStore dataStore;
        dataStore = (ElasticDataStore) contentState.getEntry().getDataStore();
        processResponse(dataStore.getClient().scroll(nextScrollId, dataStore.getScrollTime()));
    }

    private void processResponse(ElasticResponse searchResponse) {
        final int numHits = searchResponse.getNumHits();
        final List<ElasticHit> hits;
        if (numFeatures+numHits <= maxFeatures) {
            hits = searchResponse.getResults().getHits();
        } else {
            final int n = maxFeatures-numFeatures;
            hits = searchResponse.getResults().getHits().subList(0,n);
        }
        delegate = new ElasticFeatureReader(contentState, hits, searchResponse.getAggregations(), 0, null, docType);
        nextScrollId = searchResponse.getScrollId();
        lastScroll = numHits == 0 || numFeatures+hits.size()>=maxFeatures;
        LOGGER.fine("Scoll numHits=" + hits.size() + " (total=" + (numFeatures + hits.size()));
        scrollIds.add(nextScrollId);
    }

    @Override
    public SimpleFeatureType getFeatureType() {
        return delegate.getFeatureType();
    }

    @Override
    public SimpleFeature next() {
        SimpleFeature simpleFeature = simpleFeatures.get(0);
        simpleFeatures.remove(0);
        return simpleFeature;
    }

    private SimpleFeature nextInternal() throws IOException {
        final SimpleFeature feature;
        if (hasNextInternal()) {
            numFeatures++;
            feature = delegate.next();
        } else {
            throw new NoSuchElementException();
        }
        return feature;
    }

    @Override
    public boolean hasNext() {
        return !simpleFeatures.isEmpty();
    }

    private boolean hasNextInternal() throws IOException {
        if (!delegate.hasNext() && !lastScroll) {
            advanceScroll();
        }
        return (delegate.hasNext() || !lastScroll) && numFeatures<maxFeatures;
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
