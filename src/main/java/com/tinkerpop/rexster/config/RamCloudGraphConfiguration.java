package com.tinkerpop.rexster.config;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.impls.ramcloud.RamCloudGraph;
import com.tinkerpop.rexster.config.GraphConfiguration;
import org.apache.commons.configuration.Configuration;

import java.util.logging.Level;

public class RamCloudGraphConfiguration implements GraphConfiguration {

    public Graph configureGraphInstance(final Configuration properties) throws GraphConfigurationException {
        return new RamCloudGraph("infrc:host=192.168.1.101,port=12246", Level.FINER);
    }

}
