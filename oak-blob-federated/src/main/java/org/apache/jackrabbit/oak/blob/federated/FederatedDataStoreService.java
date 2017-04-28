package org.apache.jackrabbit.oak.blob.federated;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;

@Component(policy = ConfigurationPolicy.REQUIRE, name = FederatedDataStoreService.NAME)
public class FederatedDataStoreService extends AbstractFederatedDataStoreService {
    public static final String NAME = "org.apache.jackrabbit.oak.plugins.blob.datastore.FederatedDataStore";
}
