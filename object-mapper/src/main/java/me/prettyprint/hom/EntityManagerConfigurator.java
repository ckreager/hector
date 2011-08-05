package me.prettyprint.hom;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Config wrapper around the properties map required in the JPA
 * specification
 * 
 * @author zznate
 *
 */
public class EntityManagerConfigurator {
    
  private static Logger log = LoggerFactory.getLogger(EntityManagerConfigurator.class);

  public static final String PROP_PREFIX = "me.prettyprint.hom.";
  public static final String CLASSPATH_PREFIX_PROP = PROP_PREFIX + "classpathPrefix";
  public static final String CLUSTER_NAME_PROP = PROP_PREFIX + "clusterName";
  public static final String KEYSPACE_PROP = PROP_PREFIX + "keyspace";
  public static final String HOST_LIST_PROP = PROP_PREFIX + "hostList";
  public static final String CONSISTENCYLEVEL_PROP = PROP_PREFIX + "consistencylevel.";
  public static final String KEYSPACE_CONSISTENCY = CONSISTENCYLEVEL_PROP + "keyspace";
  public static final String COLUMNFAMILIES_CONSISTENCY = CONSISTENCYLEVEL_PROP + "columnfamilies";
  
  private final String classpathPrefix;
  private final String clusterName;
  private final String keyspace;
  private CassandraHostConfigurator cassandraHostConfigurator;
  private Map keySpaceConsistency;
  private Map columnFamiliesConsistency;

  
  /**
   * Construct an EntityManagerConfigurator to extract the propeties related
   * to entity management
   * @param properties
   */
  public EntityManagerConfigurator(Map<String, Object> properties) {
    this(properties, null);        
  }
  
  /**
   * Same as single argument version, but allows for (nullable) 
   * {@link CassandraHostConfigurator} to be provided explicitly
   * 
   * @param properties
   * @param cassandraHostConfigurator
   */
  public EntityManagerConfigurator(Map<String, Object> properties, 
      CassandraHostConfigurator cassandraHostConfigurator) {
    classpathPrefix = getPropertyGently(properties, CLASSPATH_PREFIX_PROP,true);
    clusterName = getPropertyGently(properties, CLUSTER_NAME_PROP,true);
    keyspace = getPropertyGently(properties, KEYSPACE_PROP,true);
    if ( cassandraHostConfigurator == null ) {
      String hostList = getPropertyGently(properties, HOST_LIST_PROP, false);
      if ( StringUtils.isNotBlank(hostList) ) {
        cassandraHostConfigurator = new CassandraHostConfigurator(hostList);
      } else {
        cassandraHostConfigurator = new CassandraHostConfigurator();
      }
    }
    log.debug("Looking for {}", KEYSPACE_CONSISTENCY);
    keySpaceConsistency = getPropertyGentlyJson(properties, KEYSPACE_CONSISTENCY, false);
    log.debug("Looking for {}", COLUMNFAMILIES_CONSISTENCY);
    columnFamiliesConsistency = getPropertyGentlyJson(properties, COLUMNFAMILIES_CONSISTENCY, false);
    this.cassandraHostConfigurator = cassandraHostConfigurator;
  }

  public static Map getJsonPropertyValue(String jsonText) throws ParseException{
    JSONParser parser = new JSONParser();
    ContainerFactory containerFactory = new ContainerFactory(){
      @Override
      public List creatArrayContainer() {
        return new LinkedList();
      }
      @Override
      public Map createObjectContainer() {
        return new LinkedHashMap();
      }
    };
    return (Map)parser.parse(jsonText, containerFactory);
  }
  
  public static Map getPropertyGentlyJson(Map<String, Object> props, String key, boolean throwError) {
    String jsonString = getPropertyGently(props, key, throwError);
    if(jsonString == null || jsonString.isEmpty()) {
        log.debug("getPropertyGentlyJson for {} was null",key);
        return Collections.EMPTY_MAP;
    }
    try {
      return getJsonPropertyValue(jsonString);
    }
    catch(ParseException pe) {
      System.out.println("Property(" + key + ") JSON Parse Exception Caught: " + pe);
    }      
    return Collections.EMPTY_MAP;
  }
  
  public static String getPropertyGently(Map<String, Object> props, String key, boolean throwError) {
    if ( props.get(key) != null ) {
      return props.get(key).toString();
    }
    if ( throwError )
      throw new IllegalArgumentException(String.format("The configuration property '%s' cannot be null.", key));
    return null;
  }

  public String getClasspathPrefix() {
    return classpathPrefix;
  }

  public String getClusterName() {
    return clusterName;
  }

  public String getKeyspace() {
    return keyspace;
  }

  public CassandraHostConfigurator getCassandraHostConfigurator() {
    return cassandraHostConfigurator;
  }

  public Map getColumnFamiliesConsistency() {
    return columnFamiliesConsistency;
  }

  public Map getKeySpaceConsistency() {
    return keySpaceConsistency;
  }
    
  @Override
  public String toString() {
    return new StringBuilder(512).append(CLASSPATH_PREFIX_PROP).append(":")
    .append(classpathPrefix).append(", ")
    .append(CLUSTER_NAME_PROP).append(":")
    .append(clusterName).append(", ")
    .append(KEYSPACE_PROP).append(":")
    .append(keyspace).toString();
  }
  
  
    
}
