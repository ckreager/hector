/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package me.prettyprint.hom;
import java.util.Collections;
import java.util.Map;
import me.prettyprint.cassandra.service.OperationType;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.factory.HFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author christopher.kreager
 */
public class ConsistencyLevelPolicyConfigurator implements ConsistencyLevelPolicy {

    private static Logger log = LoggerFactory.getLogger(EntityManagerFactoryImpl.class);
    
    public static ConsistencyLevelPolicy buildConsistencyLevelPolicy(
            EntityManagerConfigurator entityManagerConfigurator) {
        //Default
        if(entityManagerConfigurator.getColumnFamiliesConsistency().isEmpty() &&
           entityManagerConfigurator.getColumnFamiliesConsistency().isEmpty()) {
            log.debug("DefaultConsistencyLevelPolicy: ColumnFamiliesConsistency and ColumnFamiliesConsistency were empty");
            return HFactory.createDefaultConsistencyLevelPolicy();
        }
        //Custom
        ConsistencyLevelPolicyConfigurator policy = new ConsistencyLevelPolicyConfigurator();
        policy.setColumnFamilyRules(entityManagerConfigurator.getColumnFamiliesConsistency());
        policy.setKeySpaceRules(entityManagerConfigurator.getKeySpaceConsistency());
        log.debug("Custom ConsistencyLevelPolicy Created with ConsistencyLevelPolicyConfigurator");
        return policy;
    }
    
    private Map keySpaceRules = Collections.EMPTY_MAP;
    private Map columnFamilyRules = Collections.EMPTY_MAP;

    private HConsistencyLevel valueOfLevel(String level) {
        log.debug("ConsistencyLevelPolicyConfigurator valueOfLevel({})", level);
        if(level.equals("ALL"))          return HConsistencyLevel.ALL;
        if(level.equals("ANY"))          return HConsistencyLevel.ANY;
        if(level.equals("EACH_QUORUM"))  return HConsistencyLevel.EACH_QUORUM;
        if(level.equals("LOCAL_QUORUM")) return HConsistencyLevel.LOCAL_QUORUM;
        if(level.equals("ONE"))          return HConsistencyLevel.ONE;
        if(level.equals("QUORUM"))       return HConsistencyLevel.QUORUM;
        if(level.equals("THREE"))        return HConsistencyLevel.THREE;
        if(level.equals("TWO"))          return HConsistencyLevel.TWO;
        else
        return HConsistencyLevel.QUORUM;
    }
    
    @Override
    public HConsistencyLevel get(OperationType op) {
        log.debug("get keySpaceRules Rules: {}", keySpaceRules);
        return getWithRule(op, keySpaceRules);
    }

    public HConsistencyLevel getWithRule(OperationType op, Map rules) {
      switch (op){
        case READ:  
            log.debug("ConsistencyLevelPolicy.get(READ)"); 
            return valueOfLevel(rules.containsKey("READ")  ? (String) rules.get("READ")  : "");
        case WRITE: 
            log.debug("ConsistencyLevelPolicy.get(WRITE)"); 
            return valueOfLevel(rules.containsKey("WRITE") ? (String) rules.get("WRITE") : "");
        default: 
            return HConsistencyLevel.QUORUM; //Just in Case
      }
    }

    @Override
    public HConsistencyLevel get(OperationType op, String cfName) {
        if(columnFamilyRules.containsKey(cfName)) {
            Map rules = (Map) columnFamilyRules.get(cfName);
            log.debug("get ColumnFamily:{} Rules:{}", cfName, rules);
            return getWithRule(op, rules);
        }
        return HConsistencyLevel.QUORUM;
    }
    
    public void setColumnFamilyRules(Map rules) {
        log.debug("Set columnFamilyRules: {}", rules);
        this.columnFamilyRules = rules;
    }

    public void setKeySpaceRules(Map rules) {
        log.debug("Set keySpaceRules: {}", rules);
        this.keySpaceRules = rules;
    }
}
