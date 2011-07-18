/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package me.prettyprint.hom;

import java.io.IOException;
import java.net.URL;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import javax.persistence.spi.PersistenceUnitInfo;
import org.apache.commons.lang.StringUtils;
import org.apache.openjpa.lib.util.J2DoPrivHelper;
import org.apache.openjpa.persistence.PersistenceProductDerivation.ConfigurationParser;
import org.apache.openjpa.persistence.PersistenceProviderImpl;
import org.apache.openjpa.persistence.PersistenceUnitInfoImpl;

/**
 *
 * @author christopher.kreager
 */
public class PersistenceProductDerivation {
    public static final String RSRC_GLOBAL = "META-INF/openjpa.xml";
    public static final String RSRC_DEFAULT = "META-INF/persistence.xml";
    
    /**
     * Return whether the given persistence unit uses an OpenJPA provider.
     */
    private static boolean isOpenJPAPersistenceProvider
        (PersistenceUnitInfo pinfo, ClassLoader loader) {
        String provider = pinfo.getPersistenceProviderClassName();
        if (StringUtils.isEmpty(provider) 
            || CassandraPersistenceProvider.class.getName().equals(provider))
            return true;

        if (loader == null)
            loader = (ClassLoader) AccessController.doPrivileged(
                J2DoPrivHelper.getContextClassLoaderAction());
        try {
            if (PersistenceProviderImpl.class.isAssignableFrom
                (Class.forName(provider, false, loader)))
                return true;
        } catch (Throwable t) {
            System.out.println("unloadable-provider: " + provider + "Throwable:" + t.getMessage());
            return false;
        }
        return false;
    }
    
    /**
     * Find the unit with the given name, or an OpenJPA unit if no name is
     * given (preferring an unnamed OpenJPA unit to a named one).
     */
    private PersistenceUnitInfoImpl findUnit(List<PersistenceUnitInfoImpl> 
        pinfos, String name, ClassLoader loader) {
        PersistenceUnitInfoImpl ojpa = null;
        for (PersistenceUnitInfoImpl pinfo : pinfos) {
            // found named unit?
            if (name != null) {
                if (name.equals(pinfo.getPersistenceUnitName()))
                    return pinfo;
                continue;
            }

            if (isOpenJPAPersistenceProvider(pinfo, loader)) {
                // if no name given and found unnamed unit, return it.  
                // otherwise record as default unit unless we find a 
                // better match later
                if (StringUtils.isEmpty(pinfo.getPersistenceUnitName()))
                    return pinfo;
                if (ojpa == null)
                    ojpa = pinfo;
            }
        }
        return ojpa;
    }
    
    /**
     * Parse resources at the given location. Searches for a
     * PersistenceUnitInfo with the requested name, or an OpenJPA unit if
     * no name given (preferring an unnamed OpenJPA unit to a named one).
     */
    private PersistenceUnitInfoImpl parseResources(ConfigurationParser parser,
        List<URL> urls, String name, ClassLoader loader)
        throws IOException {
        List<PersistenceUnitInfoImpl> pinfos = 
            new ArrayList<PersistenceUnitInfoImpl>();
        for (URL url : urls) {
            parser.parse(url);
            pinfos.addAll((List<PersistenceUnitInfoImpl>) parser.getResults());
        }
        return findUnit(pinfos, name, loader);
    }
  
    private static List<URL>  getResourceURLs(String rsrc, ClassLoader loader)
        throws IOException {
        Enumeration<URL> urls = null;
        try {
            urls = (Enumeration) AccessController.doPrivileged(
                J2DoPrivHelper.getResourcesAction(loader, rsrc)); 
            if (!urls.hasMoreElements()) {
                if (!rsrc.startsWith("META-INF"))
                    urls = (Enumeration) AccessController.doPrivileged(
                        J2DoPrivHelper.getResourcesAction(
                            loader, "META-INF/" + rsrc)); 
                if (!urls.hasMoreElements())
                    return null;
            }
        } catch (PrivilegedActionException pae) {
            throw (IOException) pae.getException();
        }

        return Collections.list(urls);

    }  

  public PersistenceUnitInfoImpl loadPUI(String rsrc,String name, Map m) throws IOException {
    ClassLoader loader = (ClassLoader) AccessController.doPrivileged(J2DoPrivHelper.getContextClassLoaderAction());
    
    List<URL> urls = getResourceURLs(rsrc, loader);
    if (urls.isEmpty())
        return null;
    
    ConfigurationParser parser = new ConfigurationParser(m);
    PersistenceUnitInfoImpl pinfo = parseResources(parser, urls, name, loader);
    if (pinfo == null) {
        
    }
    return pinfo;
  }
    
}
