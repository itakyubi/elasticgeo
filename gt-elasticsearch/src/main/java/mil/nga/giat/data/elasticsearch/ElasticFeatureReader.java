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
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

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

    public ElasticFeatureReader(ContentState contentState, ElasticResponse response) {
        this(contentState, response.getHits(), response.getAggregations(), response.getMaxScore());
    }

    public ElasticFeatureReader(ContentState contentState, List<ElasticHit> hits, Map<String,ElasticAggregation> aggregations, float maxScore) {
        this.state = contentState;
        this.featureType = state.getFeatureType();
        this.searchHitIterator = hits.iterator();
        this.builder = new SimpleFeatureBuilder(featureType);
        this.parserUtil = new ElasticParserUtil();
        this.maxScore = maxScore;

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
    }

    @Override
    public SimpleFeatureType getFeatureType() {
        return this.featureType;
    }

    @Override
    public SimpleFeature next() {
        final String id;
        if (searchHitIterator.hasNext()) {
            id = nextHit();
        } else {
            nextAggregation();
            id = null;
        }
        return builder.buildFeature(id);
    }

    private String nextHit() {
        final ElasticHit hit = searchHitIterator.next();
        final SimpleFeatureType type = getFeatureType();
        final Map<String, Object> source = hit.getSource();

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

			// debug for wkb base64	+++
            /*
            if (sourceName.equals("_id")) {
                parserUtil.setId(hit.getId());
            }
            if (values != null && sourceName.equals("gid")) {
                parserUtil.setGid((Integer) values.get(0));
            }
             */
			// debug for wkb base64	---
            LOGGER.fine("sourceName:" + sourceName);
            Geometry tmpGeometry1 = null;
            Geometry tmpGeometry2 = null;
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
            } else if (values != null && sourceName.equals("wkb_shape")) {
                LOGGER.fine("wkb name:" + name);
                tmpGeometry1 = parserUtil.createGeometryFromWkb(values.get(0));
                //builder.set(name, parserUtil.createGeometryFromWkb(values.get(0)));
                LOGGER.fine("0 tmpGeometry1:");
                builder.set("shape", tmpGeometry1);
            } else if (values != null && Geometry.class.isAssignableFrom(descriptor.getType().getBinding())) {
                /*
                if (values.size() == 1) {
                    LOGGER.fine("values.size() == 1 wkt name:" + name);
                    //tmpGeometry2 = parserUtil.createGeometry(values.get(0));
                    //builder.set(name, parserUtil.createGeometry(values.get(0)));
                    LOGGER.fine("1 tmpGeometry1:" + tmpGeometry1);
                } else {
                    LOGGER.fine("else wkt name:" + name);
                    builder.set(name, parserUtil.createGeometry(values));
                }*/
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

        return state.getEntry().getTypeName() + "." + hit.getId();
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
        return searchHitIterator.hasNext() || aggregationIterator.hasNext();
    }

    @Override
    public void close() {
        builder = null;
        searchHitIterator = null;
    }

}
