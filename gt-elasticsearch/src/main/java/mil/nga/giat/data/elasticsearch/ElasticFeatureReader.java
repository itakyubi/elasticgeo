/*
 * This file is hereby placed into the Public Domain. This means anyone is
 * free to do whatever they wish with this file.
 */
package mil.nga.giat.data.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;

import mil.nga.giat.data.elasticsearch.ElasticDataStore.ArrayEncoding;
import mil.nga.giat.shaded.es.common.joda.Joda;
import mil.nga.giat.shaded.joda.time.format.DateTimeFormatter;

import static mil.nga.giat.data.elasticsearch.ElasticConstants.DATE_FORMAT;
import static mil.nga.giat.data.elasticsearch.ElasticConstants.FULL_NAME;

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

/**
 * FeatureReader access to the Elasticsearch index.
 */
class ElasticFeatureReader implements FeatureReader<SimpleFeatureType, SimpleFeature> {

    private final static Logger LOGGER = Logging.getLogger(ElasticFeatureReader.class);

    private final ContentState state;

    private final SimpleFeatureType featureType;

    private float maxScore;

    private ObjectMapper mapper;

    private ArrayEncoding arrayEncoding;

    private SimpleFeatureBuilder builder;

    private Iterator<ElasticHit> searchHitIterator;

    private Iterator<Map<String,Object>> aggregationIterator;

    private ElasticParserUtil parserUtil;

    private final List<SimpleFeature> simpleFeatures;

    private String docType;

    public ElasticFeatureReader(ContentState contentState,List<SimpleFeature> cachedSimpleFeatures) {
        this.state = contentState;
        this.featureType = state.getFeatureType();
        // fill simpleFeatures
        this.simpleFeatures = new ArrayList<SimpleFeature>();
        if((cachedSimpleFeatures != null) && (!cachedSimpleFeatures.isEmpty())) {
            simpleFeatures.addAll(cachedSimpleFeatures);
        }
    }

    public ElasticFeatureReader(ContentState contentState, ElasticResponse response, List<SimpleFeature> cachedSimpleFeatures, String docType) {
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
        ElasticFeatureSource.setSimpleFeatures(this.docType,Integer.valueOf(id),sf);
        return sf;
    }

    private String nextHitInternal() {
        final ElasticHit hit = searchHitIterator.next();
        //final SimpleFeatureType type = getFeatureType();
        final Map<String, Object> source = hit.getSource();

		builder.set("shape", parserUtil.createGeometry(source.get("shape")));
		
		return hit.getId();
		/*
        final Float score;
        final Float relativeScore;
        if (hit.getScore() != null && !Float.isNaN(hit.getScore()) && maxScore>0) {
            score = hit.getScore();
            relativeScore = score / maxScore;
        } else {
            score = null;
            relativeScore = null;
        }

        for (final AttributeDescriptor descriptor : type.getAttributeDescriptors()) {
            final String name = descriptor.getType().getName().getLocalPart();
            final String sourceName = (String) descriptor.getUserData().get(FULL_NAME);

            List<Object> values = hit.field(sourceName);
            if (values == null && source != null) {
                // read field from source
                values = parserUtil.readField(source, sourceName);
            }

            if (values == null && sourceName.equals("_id")) {
                builder.set(name, hit.getId());
            } else if (values == null && sourceName.equals("_index")) {
                builder.set(name, hit.getIndex());
            } else if (values == null && sourceName.equals("_type")) {
                builder.set(name, hit.getType());
            } else if (values == null && sourceName.equals("_score")) {
                builder.set(name, score);
            } else if (values == null && sourceName.equals("_relative_score")) {
                builder.set(name, relativeScore);
            } else if (values != null && Geometry.class.isAssignableFrom(descriptor.getType().getBinding())) {
                if (values.size() == 1) {
                    builder.set(name, parserUtil.createGeometry(values.get(0)));
                } else {
                    builder.set(name, parserUtil.createGeometry(values));
                }
            } else if (values != null && Date.class.isAssignableFrom(descriptor.getType().getBinding())) {
                Object dataVal = values.get(0);
                if (dataVal instanceof Double) {
                    builder.set(name, new Date(Math.round((Double) dataVal)));
                } else if (dataVal instanceof Integer) {
                    builder.set(name, new Date((Integer) dataVal));
                } else if (dataVal instanceof Long) {
                    builder.set(name, new Date((long) dataVal));
                } else {
                    final String format = (String) descriptor.getUserData().get(DATE_FORMAT);
                    final DateTimeFormatter dateFormatter = Joda.forPattern(format).parser();

                    Date date = dateFormatter.parseDateTime((String) dataVal).toDate();
                    builder.set(name, date);
                }
            } else if (values != null && values.size() == 1) {
                builder.set(name, values.get(0));
            } else if (values != null && !name.equals("_aggregation")) {
                final Object value;
                if (arrayEncoding == ArrayEncoding.CSV) {
                    // only include first array element when using CSV array encoding
                    value = values.get(0);
                } else {
                    value = values;
                }
                builder.set(name, value);
            }
        }
        return hit.getId();
        //return state.getEntry().getTypeName() + "." + hit.getId();
		*/
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
