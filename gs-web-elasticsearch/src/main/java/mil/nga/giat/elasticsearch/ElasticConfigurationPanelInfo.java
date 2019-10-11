/* (c) 2014 Open Source Geospatial Foundation - all rights reserved
 * This code is licensed under the GPL 2.0 license, available at the root
 * application directory.
 */

package mil.nga.giat.elasticsearch;

import org.geoserver.catalog.FeatureTypeInfo;
import org.geoserver.platform.ExtensionPriority;
import org.geoserver.web.data.resource.ResourceConfigurationPanelInfo;

/**
 * 
 * Implements ResourceConfigurationPanelInfo extension point to add Elasticsearch 
 * attribute configuration link on resource page. <br>
 * Priority is reduced under standard {@link ExtensionPriority#LOWEST} to shows
 * the Elasticsearch link after other panels.
 * 
 */
class ElasticConfigurationPanelInfo extends ResourceConfigurationPanelInfo implements ExtensionPriority {

    private static final long serialVersionUID = 1485404586629946126L;

    @Override
    public boolean canHandle(Object obj) {
        boolean canHandle = false;
        if (obj instanceof FeatureTypeInfo) {
            FeatureTypeInfo fti = (FeatureTypeInfo) obj;
            for (String st : getSupportedTypes()) {
                if (fti.getStore().getType().equals(st)) {
                    canHandle = true;
                    break;
                }
            }
        }
        return canHandle;
    }

    @Override
    public int getPriority() {
        return ExtensionPriority.LOWEST + 1;
    }

}
