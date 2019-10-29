/*
 * This file is hereby placed into the Public Domain. This means anyone is
 * free to do whatever they wish with this file.
 */
package mil.nga.giat.data.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import mil.nga.giat.data.elasticsearch.ElasticDataStore.ArrayEncoding;
import org.geotools.data.FeatureReader;
import org.geotools.data.store.ContentState;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.util.logging.Logging;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

import static mil.nga.giat.data.elasticsearch.ElasticConstants.FULL_NAME;

/**
 * FeatureReader access to the Elasticsearch index.
 */
class ElasticFeatureReader implements FeatureReader<SimpleFeatureType, SimpleFeature> {

    private final static Logger LOGGER = Logging.getLogger(ElasticFeatureReader.class);

    private final ContentState state;

    private final SimpleFeatureType featureType;

    private final float maxScore;

    private final ObjectMapper mapper;

    private final ArrayEncoding arrayEncoding;

    private SimpleFeatureBuilder builder;

    private Iterator<ElasticHit> searchHitIterator;

    private Iterator<Map<String,Object>> aggregationIterator;

    private final ElasticParserUtil parserUtil;

    private final List<SimpleFeature> simpleFeatures;

    private final Map<String,SimpleFeature> newCacheSimpleFeatures = new HashMap<String,SimpleFeature>();

    private final String docType;

    public ElasticFeatureReader(ContentState contentState, ElasticResponse response,List<SimpleFeature> cachedSimpleFeatures,String docType) {
        this(contentState, response.getHits(), response.getAggregations(), response.getMaxScore(),cachedSimpleFeatures,docType);
    }

    public ElasticFeatureReader(ContentState contentState, List<ElasticHit> hits, Map<String,ElasticAggregation> aggregations, float maxScore,List<SimpleFeature> cachedSimpleFeatures,String docType) {
        this.state = contentState;
        this.featureType = state.getFeatureType();
        this.searchHitIterator = hits.iterator();
        this.builder = new SimpleFeatureBuilder(featureType);
        this.parserUtil = new ElasticParserUtil();
        this.maxScore = maxScore;
        this.docType = docType;

        this.aggregationIterator = Collections.emptyIterator();
        if (aggregations != null && !aggregations.isEmpty()) {
            String aggregationName = aggregations.keySet().stream().findFirst().orElse(null);
            if (aggregations.size() > 1) {
                LOGGER.info("Result has multiple aggregations. Using " + aggregationName);
            }
            if (aggregations.get(aggregationName).getBuckets() != null) {
                this.aggregationIterator = aggregations.get(aggregationName).getBuckets().iterator();
            }
        }

        if (contentState.getEntry() != null && contentState.getEntry().getDataStore() != null) {
            final ElasticDataStore dataStore;
            dataStore = (ElasticDataStore) contentState.getEntry().getDataStore();
            this.arrayEncoding = dataStore.getArrayEncoding();
        } else {
            this.arrayEncoding = ArrayEncoding.valueOf((String) ElasticDataStoreFactory.ARRAY_ENCODING.getDefaultValue());
        }

        this.mapper = new ObjectMapper();

        // fill simpleFeatures
        this.simpleFeatures = new ArrayList<SimpleFeature>();
        if((cachedSimpleFeatures != null) && (!cachedSimpleFeatures.isEmpty())) {
            simpleFeatures.addAll(cachedSimpleFeatures);
        }
        while(hasNextInternal()) {
            simpleFeatures.add(nextInternal());
        }
    }

    public Map<String, SimpleFeature> getNewCacheSimpleFeatures() {
        return newCacheSimpleFeatures;
    }

    @Override
    public SimpleFeatureType getFeatureType() {
        return this.featureType;
    }

    @Override
    public SimpleFeature next() {
        SimpleFeature simpleFeature = simpleFeatures.get(0);
        simpleFeatures.remove(0);
        return simpleFeature;
    }

    private SimpleFeature nextInternal() {
        final String id;
        if (searchHitIterator.hasNext()) {
            id = nextHitInternal();
        } else {
            nextAggregation();
            id = null;
        }

        String buildId = state.getEntry().getTypeName() + "." + id;
        SimpleFeature sf = builder.buildFeature(buildId);
        // cache simplefeatures
        ElasticFeatureSource.setSimpleFeatures((this.docType + "/" + id),sf);
        return sf;
    }

    private String nextHitInternal() {
        final ElasticHit hit = searchHitIterator.next();
        //final SimpleFeatureType type = getFeatureType();
        final Map<String, Object> source = hit.getSource();
        Geometry tmpGeometry = parserUtil.createGeometryFromWkb(source.get("wkb"));
        builder.set("shape", tmpGeometry);
        return hit.getId();
    }

    private void nextAggregation() {
        final Map<String, Object> aggregation = aggregationIterator.next();
        try {
            final byte[] data = mapper.writeValueAsBytes(aggregation);
            builder.set("_aggregation", data);
        } catch (IOException e) {
            LOGGER.warning("Unable to set aggregation. Try reloading layer.");
        }
    }

    @Override
    public boolean hasNext() {
        return !simpleFeatures.isEmpty();
    }

    private boolean hasNextInternal() {
        return searchHitIterator.hasNext() || aggregationIterator.hasNext();
    }

    @Override
    public void close() {
        builder = null;
        searchHitIterator = null;
    }

}
