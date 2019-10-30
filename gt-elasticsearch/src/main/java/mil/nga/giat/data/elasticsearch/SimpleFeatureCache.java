package mil.nga.giat.data.elasticsearch;

import org.opengis.feature.simple.SimpleFeature;

import java.util.HashMap;
import java.util.Map;

public class SimpleFeatureCache {
    private final Map<Integer, SimpleFeature> cache = new HashMap<Integer, SimpleFeature>();

    public void put(Integer id,SimpleFeature simpleFeature) {
        cache.put(id,simpleFeature);
    }

    public boolean containsKey(Integer key) {
        return cache.containsKey(key);
    }

    SimpleFeature get(Integer key) {
        return cache.get(key);
    }
}
