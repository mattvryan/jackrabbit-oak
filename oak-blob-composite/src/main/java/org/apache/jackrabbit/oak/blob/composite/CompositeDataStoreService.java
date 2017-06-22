package org.apache.jackrabbit.oak.blob.composite;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;

@Component(policy = ConfigurationPolicy.REQUIRE, name = CompositeDataStoreService.NAME)
public class CompositeDataStoreService extends AbstractCompositeDataStoreService {
    public static final String NAME = "org.apache.jackrabbit.oak.plugins.blob.datastore.CompositeDataStore";
}
