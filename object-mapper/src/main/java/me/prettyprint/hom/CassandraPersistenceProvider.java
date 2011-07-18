/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package me.prettyprint.hom;

import java.util.Collections;
import java.util.Map;
import javax.persistence.EntityManagerFactory;
import javax.persistence.spi.PersistenceProvider;
import javax.persistence.spi.PersistenceUnitInfo;
import javax.persistence.spi.ProviderUtil;
import org.apache.openjpa.persistence.PersistenceUnitInfoImpl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraPersistenceProvider implements PersistenceProvider {
  private static Logger log = LoggerFactory.getLogger(CassandraPersistenceProvider.class);
  
  private Map<String, Object> defProperties;

    public void setDefProperties(Map<String, Object> defProperties) {
        this.defProperties = defProperties;
    }
  
  public CassandraPersistenceProvider() {
    this(Collections.EMPTY_MAP);
    System.out.println("init CassandraPersistenceProvider()");
  }
  
  public CassandraPersistenceProvider(Map<String, Object> map) {
    this.defProperties = map;
    System.out.println("init CassandraPersistenceProvider(map)");
  }
  
  @Override
  public EntityManagerFactory createContainerEntityManagerFactory(
      PersistenceUnitInfo info, Map map) {
    System.out.println("called createContainerEntityManagerFactory(info,map)");
    if ( log.isDebugEnabled() ) {
      log.debug("creating EntityManagerFactory {} with properties {} ", "null", map);
    }
    // TODO Auto-generated method stub
    return null;
  }
 
  @Override
  public EntityManagerFactory createEntityManagerFactory(String emName, Map map) {
    System.out.println("called createEntityManagerFactory(emName=" + emName + ", map)");
    if ( log.isDebugEnabled() ) {
      log.debug("creating EntityManagerFactory {} with properties {} ", emName, map);
    }
    if ( map == null || map.isEmpty()) {
        
        PersistenceProductDerivation pd = new PersistenceProductDerivation();
        try {
            PersistenceUnitInfoImpl pinfo = pd.loadPUI(PersistenceProductDerivation.RSRC_DEFAULT, emName, map);
            if(pinfo == null) {
                System.out.println("called loadPUI(rsrc="+PersistenceProductDerivation.RSRC_DEFAULT+",emName="+emName+",map), return null"); 
                return null;
            }
            this.defProperties = pinfo.toOpenJPAProperties();
        }
        catch(Exception ex) {
            System.out.println("called loadPUI(rsrc="+PersistenceProductDerivation.RSRC_DEFAULT+",emName="+emName+",map), Caught Exception: \n" +ex.getMessage());
            return null;
        }
        
      return new EntityManagerFactoryImpl(defProperties);
    }
    return new EntityManagerFactoryImpl(map);
  }

  @Override
  public ProviderUtil getProviderUtil() {
      System.out.println("called getProviderUtil()");
    // TODO Auto-generated method stub
    return null;
  }

}