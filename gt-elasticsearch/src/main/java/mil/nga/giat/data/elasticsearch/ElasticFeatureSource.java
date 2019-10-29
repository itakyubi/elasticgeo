/*
 * This file is hereby placed into the Public Domain. This means anyone is
 * free to do whatever they wish with this file.
 */
package mil.nga.giat.data.elasticsearch;


import org.geotools.data.FeatureReader;
import org.geotools.data.FilteringFeatureReader;
import org.geotools.data.Query;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.filter.visitor.ExtractBoundsFilterVisitor;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.util.logging.Logging;
import org.locationtech.jts.geom.Envelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterVisitor;
import org.opengis.filter.sort.SortBy;
import org.opengis.filter.sort.SortOrder;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Provides access to a specific type within the Elasticsearch index described
 * by the associated data store.
 *
 */
class ElasticFeatureSource extends ContentFeatureSource {

    private final static Logger LOGGER = Logging.getLogger(ElasticFeatureSource.class);

    private Boolean filterFullySupported;

    private final static Map<String,Object> indexCaches = new HashMap<String,Object>();

    public ElasticFeatureSource(ContentEntry entry, Query query) throws IOException {
        super(entry, query);

        final ElasticDataStore dataStore = getDataStore();
        if (dataStore.getLayerConfigurations().get(entry.getName().getLocalPart()) == null) {
            final List<ElasticAttribute> attributes = dataStore.getElasticAttributes(entry.getName());
            final ElasticLayerConfiguration config = new ElasticLayerConfiguration(entry.getName().getLocalPart());
            config.getAttributes().addAll(attributes);
            dataStore.setLayerConfiguration(config);
        }
    }

    /**
     * Access parent datastore
     */
    public ElasticDataStore getDataStore() {
        return (ElasticDataStore) super.getDataStore();
    }

    /**
     * Implementation that generates the total bounds
     */
    @Override
    protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException {
        LOGGER.fine("getBoundsInternal");
        final CoordinateReferenceSystem crs = getSchema().getCoordinateReferenceSystem();
        final ReferencedEnvelope bounds = new ReferencedEnvelope(crs);

        try (FeatureReader<SimpleFeatureType, SimpleFeature> featureReader = getReaderInternal(query)) {
            while (featureReader.hasNext()) {
                final SimpleFeature feature = featureReader.next();
                bounds.include(feature.getBounds());
            }
        }
        return bounds;
    }

    @Override
    protected int getCountInternal(Query query) throws IOException {
        LOGGER.fine("getCountInternal");
        int hits = 0;
        final ElasticRequest searchRequest = prepareSearchRequest(query, false);
        try {
            if (!filterFullySupported) {
                try (FeatureReader<SimpleFeatureType, SimpleFeature> reader = getReaderInternal(query)) {
                    while (reader.hasNext()) {
                        reader.next();
                        hits++;
                    }
                }
            } else {
                searchRequest.setSize(0);
                final ElasticDataStore dataStore = getDataStore();
                final String docType = dataStore.getDocType(entry.getName());
                final ElasticResponse sr = dataStore.getClient().search(dataStore.getIndexName(), docType, searchRequest);
                final int totalHits = (int) sr.getTotalNumHits();
                final int size = getSize(query);
                final int from = getStartIndex(query);
                hits = Math.max(0, Math.min(totalHits - from, size));
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
            throw new IOException("Error executing count search", e);
        }

        return hits;
    }

    public static void setSimpleFeatures(String docType,String id,SimpleFeature simpleFeature) {
        synchronized (ElasticFeatureSource.class) {
            // 检查index是否cache
            if (!indexCaches.containsKey(docType)) {
                Map<String, SimpleFeature> simpleFeatures = new HashMap<String, SimpleFeature>();
                indexCaches.put(docType, simpleFeatures);
            }
            // 得到当前index的cache
            Map<String, SimpleFeature> simpleFeatures = (Map<String, SimpleFeature>) indexCaches.get(docType);
            simpleFeatures.put(id, simpleFeature);
        }
    }

    @Override
    protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query) throws IOException {
        LOGGER.fine("getReaderInternal");
        FeatureReader<SimpleFeatureType, SimpleFeature> reader;
        try {
            final ElasticDataStore dataStore = getDataStore();
            final String docType = dataStore.getDocType(entry.getName());
            final boolean scroll = !useSortOrPagination(query) && dataStore.getScrollEnabled();
            final boolean firstSearchScroll = false;
            final ElasticRequest searchRequest = prepareSearchRequest(query, firstSearchScroll);
            searchRequest.setSourceShow(false);
			LOGGER.fine("call 1 search +++" + Thread.currentThread().getName());
            final ElasticResponse sr = dataStore.getClient().search(dataStore.getIndexName(), docType, searchRequest);	
			LOGGER.fine("call 1 search ---" + Thread.currentThread().getName());
			//LOGGER.fine("call 1 search ---");
            if (LOGGER.isLoggable(Level.FINE)) {
                //LOGGER.fine("Search response 1: " + sr);
            }
            //得到id
            List<ElasticHit> hits = sr.getHits();
            // 未找到cache的id
            List<String> Ids = new ArrayList<String>();
            // 根据id找到的cache
            List<SimpleFeature> cachedSimpleFeatures = new ArrayList<SimpleFeature>();
            // 当前index的cache
            Map<String, SimpleFeature> simpleFeatures;
            synchronized (ElasticFeatureSource.class) {
                // 检查index是否cache
                if (!indexCaches.containsKey(docType)) {
                    Map<String, SimpleFeature> newSimpleFeatures = new HashMap<String, SimpleFeature>();
                    indexCaches.put(docType, newSimpleFeatures);
                }
                // 得到当前index的cache
                simpleFeatures = (Map<String, SimpleFeature>) indexCaches.get(docType);
            }
            for (ElasticHit hit:hits) {
                // 检测cache里面是否包含id对应的simpleFeature
                String id = hit.getId();
                if(simpleFeatures.containsKey(id)) {
                    cachedSimpleFeatures.add(simpleFeatures.get(id));
                } else {
                    Ids.add(hit.getId());
                }
            }
            //LOGGER.fine("Ids:" + Ids.toString());
            //请求wkb
            //LOGGER.fine("dataStore.getIndexName():" + dataStore.getIndexName());
            //LOGGER.fine("docType:" + docType);
            //LOGGER.fine("searchRequest.getQuery():" + searchRequest.getQuery().toString());
            //LOGGER.fine("filter.ClassName:" + searchRequest.getQuery().getClass().getName());
            Map<String,Object> newQuery = new HashMap<>();
            Map<String,Object> filter = new HashMap<>();
            Map<String,Object> terms = new HashMap<>();
            Map<String,Object> bool = new HashMap<>();
            terms.put("_id",Ids);
            filter.put("terms",terms);
            bool.put("filter",filter);
            newQuery.put("bool",bool);
            searchRequest.setQuery(newQuery);
            searchRequest.setSourceShow(true);
            // set second scroll size
            if(scroll) {
                if (dataStore.getScrollSize() != null) {
                    searchRequest.setSize(dataStore.getScrollSize().intValue());
                }
                if (dataStore.getScrollTime() != null) {
                    searchRequest.setScroll(dataStore.getScrollTime());
                }
            }

            String indexNameWkb = docType + "_wkb";//dataStore.getIndexName();
            String docTypeWkb = docType;
            LOGGER.fine("call 2 search +++" + Thread.currentThread().getName());
            final ElasticResponse srWkb = dataStore.getClient().search(indexNameWkb, docTypeWkb, searchRequest);
            LOGGER.fine("call 2 search ---" + Thread.currentThread().getName());
            //LOGGER.fine("call 2 search ---");
            //LOGGER.fine("Search response 2 srWkb: " + srWkb);
            //LOGGER.fine("Search response 2 sr: " + sr);

            if (!scroll) {
                reader = new ElasticFeatureReader(getState(), srWkb,cachedSimpleFeatures,docType);
            } else {
                reader = new ElasticFeatureReaderScroll(getState(), srWkb, getSize(query),cachedSimpleFeatures,docType);
            }
            // update cache

            if (!filterFullySupported) {
                reader = new FilteringFeatureReader<>(reader, query.getFilter());
            }

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
            throw new IOException("Error executing query search", e);
        }
        return reader;
    }

    private ElasticRequest prepareSearchRequest(Query query, boolean scroll) throws IOException {
        String naturalSortOrder = SortOrder.ASCENDING.toSQL().toLowerCase();
        final ElasticRequest searchRequest = new ElasticRequest();
        final ElasticDataStore dataStore = getDataStore();
        final String docType = dataStore.getDocType(entry.getName());

        LOGGER.fine("Preparing " + docType + " (" + entry.getName() + ") query");
        if (!scroll) {
            if (query.getSortBy()!=null){
                for (final SortBy sort : query.getSortBy()) {
                    final String sortOrder = sort.getSortOrder().toSQL().toLowerCase();
                    if (sort.getPropertyName() != null) {
                        final String name = sort.getPropertyName().getPropertyName();
                        searchRequest.addSort(name, sortOrder);
                    } else {
                        naturalSortOrder = sortOrder;
                    }
                }
            }

            // pagination
            searchRequest.setSize(getSize(query));
            searchRequest.setFrom(getStartIndex(query));
        } else {
            if (dataStore.getScrollSize() != null) {
                searchRequest.setSize(dataStore.getScrollSize().intValue());
            }
            if (dataStore.getScrollTime() != null) {
                searchRequest.setScroll(dataStore.getScrollTime());
            }
        }

        if (dataStore.isSourceFilteringEnabled()) {
            if (query.getProperties() != Query.ALL_PROPERTIES) {
                for (String property : query.getPropertyNames()) {
                    searchRequest.addSourceInclude(property);
                }
            } else {
                // add source includes
                setSourceIncludes(searchRequest);
            }
        }

        // add query and post filter
        final FilterToElastic filterToElastic = new FilterToElastic();
        filterToElastic.setFeatureType(buildFeatureType());
        filterToElastic.encode(query);
        filterFullySupported = filterToElastic.getFullySupported();
        if (!filterFullySupported) {
            LOGGER.fine("Filter is not fully supported by native Elasticsearch."
                    + " Additional post-query filtering will be performed.");
        }
        final Map<String,Object> queryBuilder = filterToElastic.getQueryBuilder();

        final Map<String,Object> nativeQueryBuilder = filterToElastic.getNativeQueryBuilder();

        searchRequest.setQuery(queryBuilder);

        if (isSort(query) && nativeQueryBuilder.equals(ElasticConstants.MATCH_ALL)) {
            final String sortKey = dataStore.getClient().getVersion() < 7 ? "_uid" : "_id";
            searchRequest.addSort(sortKey, naturalSortOrder);
        }

        if (filterToElastic.getAggregations() != null) {
            final Map<String, Map<String, Map<String, Object>>> aggregations = filterToElastic.getAggregations();
            final Envelope envelope = (Envelope) query.getFilter().accept(ExtractBoundsFilterVisitor.BOUNDS_VISITOR, null);
            final long gridSize;
            if (dataStore.getGridSize() != null) {
                gridSize = dataStore.getGridSize();
            } else {
                gridSize = (Long) ElasticDataStoreFactory.GRID_SIZE.getDefaultValue();
            }
            final double gridThreshold;
            if (dataStore.getGridThreshold() != null) {
                gridThreshold = dataStore.getGridThreshold();
            } else {
                gridThreshold = (Double) ElasticDataStoreFactory.GRID_THRESHOLD.getDefaultValue();
            }
            final int precision = GeohashUtil.computePrecision(envelope, gridSize, gridThreshold);
            LOGGER.fine("Updating GeoHash grid aggregation precision to " + precision);
            GeohashUtil.updateGridAggregationPrecision(aggregations, precision);
            searchRequest.setAggregations(aggregations);
            searchRequest.setSize(0);
        }

        return searchRequest;
    }

    private void setSourceIncludes(final ElasticRequest searchRequest) throws IOException {
        final ElasticDataStore dataStore = getDataStore();
        final List<ElasticAttribute> attributes = dataStore.getElasticAttributes(entry.getName());
        for (final ElasticAttribute attribute : attributes) {
            if (attribute.isUse() && attribute.isStored()) {
                searchRequest.addField(attribute.getName());
            } else if (attribute.isUse()) {
                searchRequest.addSourceInclude(attribute.getName());
            }
        }
    }

    private boolean isSort(Query query) {
        return query.getSortBy() != null && query.getSortBy().length > 0;
    }

    private boolean useSortOrPagination(Query query) {
        return (query.getSortBy() != null && query.getSortBy().length > 0) ||
                query.getStartIndex()!=null;
    }

    private int getSize(Query query) {
        final int size;
        if (!query.isMaxFeaturesUnlimited()) {
            size = query.getMaxFeatures();
        } else {
            size = getDataStore().getDefaultMaxFeatures();
            LOGGER.fine("Unlimited maxFeatures not supported. Using default: " + size);
        }
        return size;
    }

    private int getStartIndex(Query query) {
        final int from;
        if (query.getStartIndex() != null) {
            from = query.getStartIndex();
        } else {
            from = 0;
        }
        return from;
    }

    @Override
    protected SimpleFeatureType buildFeatureType() {
        final ElasticDataStore ds = getDataStore();
        final ElasticLayerConfiguration layerConfig;
        layerConfig = ds.getLayerConfigurations().get(entry.getTypeName());
        final List<ElasticAttribute> attributes;
        if (layerConfig != null) {
            attributes = layerConfig.getAttributes();
        } else {
            attributes = null;
        }

        final ElasticFeatureTypeBuilder typeBuilder;
        typeBuilder = new ElasticFeatureTypeBuilder(attributes, entry.getName());
        return typeBuilder.buildFeatureType();
    }

    @Override
    protected boolean canLimit() {
        return true;
    }

    @Override
    protected boolean canOffset() {
        return true;
    }

    @Override
    protected boolean canFilter() {
        return true;
    }

    @Override
    protected boolean canSort() {
        return true;
    }

}
