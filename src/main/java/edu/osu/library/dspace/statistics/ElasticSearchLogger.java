package edu.osu.library.dspace.statistics;


import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.log4j.Logger;
import org.dspace.content.*;
import org.dspace.content.Collection;
import org.dspace.core.ConfigurationManager;
import org.dspace.core.Constants;
import org.dspace.eperson.EPerson;
import org.dspace.statistics.util.DnsLookup;
import org.dspace.statistics.util.LocationUtils;
import org.dspace.statistics.util.SpiderDetector;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;


import javax.servlet.http.HttpServletRequest;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

public class ElasticSearchLogger {

    private static Logger log = Logger.getLogger(ElasticSearchLogger.class);

    private static final boolean useProxies;

    public static final String DATE_FORMAT_8601 = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    public static final String DATE_FORMAT_DCDATE = "yyyy-MM-dd'T'HH:mm:ss'Z'";

    private static final LookupService locationService;

    private static Map<String, String> metadataStorageInfo;

    static {

        LookupService service = null;
        // Get the db file for the location
        String dbfile = ConfigurationManager.getProperty("solr-statistics", "dbfile");
        if (dbfile != null) {
            try {
                service = new LookupService(dbfile,
                        LookupService.GEOIP_STANDARD);
            } catch (FileNotFoundException fe) {
                log.error("The GeoLite Database file is missing (" + dbfile + ")! Solr Statistics cannot generate location based reports! Please see the DSpace installation instructions for instructions to install this file.", fe);
            } catch (IOException e) {
                log.error("Unable to load GeoLite Database file (" + dbfile + ")! You may need to reinstall it. See the DSpace installation instructions for more details.", e);
            }
        } else {
            log.error("The required 'dbfile' configuration is missing in solr-statistics.cfg!");
        }
        locationService = service;

        if ("true".equals(ConfigurationManager.getProperty("useProxies"))) {
            useProxies = true;
        } else {
            useProxies = false;
        }

        log.info("useProxies=" + useProxies);

        metadataStorageInfo = new HashMap<String, String>();
        int count = 1;
        String metadataVal;
        while ((metadataVal = ConfigurationManager.getProperty("solr-statistics", "metadata.item." + count)) != null) {
            String storeVal = metadataVal.split(":")[0];
            String metadataField = metadataVal.split(":")[1];

            metadataStorageInfo.put(storeVal, metadataField);
            log.info("solr-statistics.metadata.item." + count + "=" + metadataVal);
            count++;
        }
    }

    public static void post(DSpaceObject dspaceObject, HttpServletRequest request, EPerson currentUser) {

        log.info("hi from fsdfsdfsdf logger");


        String indexName = "kb";
        String indexType = "stats";

        Client client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("127.0.0.1", 9300));
        //   client.admin().indices().create(new CreateIndexRequest(indexName)).actionGet();

        //   client.admin().cluster().health(new ClusterHealthRequest(indexName).waitForYellowStatus()).actionGet();


        boolean isSpiderBot = SpiderDetector.isSpider(request);

        try {
            if (isSpiderBot &&
                    !ConfigurationManager.getBooleanProperty("solr-statistics", "logBots", true)) {
                return;
            }


            // Save our basic info that we already have

            String ip = request.getRemoteAddr();

            if (isUseProxies() && request.getHeader("X-Forwarded-For") != null) {
                /* This header is a comma delimited list */
                for (String xfip : request.getHeader("X-Forwarded-For").split(",")) {
                    /* proxy itself will sometime populate this header with the same value in
                        remote address. ordering in spec is vague, we'll just take the last
                        not equal to the proxy
                    */
                    if (!request.getHeader("X-Forwarded-For").contains(ip)) {
                        ip = xfip.trim();
                    }
                }
            }

            XContentBuilder docBuilder = null;


            docBuilder = XContentFactory.jsonBuilder().startObject();


            docBuilder.field("ip", ip);

            docBuilder.field("id", dspaceObject.getID());

            docBuilder.field("typeIndex", dspaceObject.getType());

            docBuilder.field("type", Constants.typeText[dspaceObject.getType()]);

            // Save the current time
            docBuilder.field("time", DateFormatUtils.format(new Date(), DATE_FORMAT_8601));
            if (currentUser != null) {
                docBuilder.field("epersonid", currentUser.getID());
            }

            try {
                String dns = DnsLookup.reverseDns(ip);
                docBuilder.field("dns", dns.toLowerCase());
            } catch (Exception e) {
                log.error("Failed DNS Lookup for IP:" + ip);
                log.debug(e.getMessage(), e);
            }

            // Save the location information if valid, save the event without
            // location information if not valid
            Location location = locationService.getLocation(ip);
            if (location != null
                    && !("--".equals(location.countryCode)
                    && location.latitude == -180 && location.longitude == -180)) {
                try {
                    docBuilder.field("continent", LocationUtils
                            .getContinentCode(location.countryCode));
                } catch (Exception e) {
                    System.out
                            .println("COUNTRY ERROR: " + location.countryCode);
                }
                docBuilder.field("countryCode", location.countryCode);
                docBuilder.field("city", location.city);
                docBuilder.field("latitude", location.latitude);
                docBuilder.field("longitude", location.longitude);
                docBuilder.field("isBot", isSpiderBot);

                if (request.getHeader("User-Agent") != null) {
                    docBuilder.field("userAgent", request.getHeader("User-Agent"));
                }
            }

            if (dspaceObject instanceof Item) {
                Item item = (Item) dspaceObject;
                // Store the metadata
                for (Object storedField : metadataStorageInfo.keySet()) {
                    String dcField = metadataStorageInfo
                            .get(storedField);

                    DCValue[] vals = item.getMetadata(dcField.split("\\.")[0],
                            dcField.split("\\.")[1], dcField.split("\\.")[2],
                            Item.ANY);
                    for (DCValue val1 : vals) {
                        String val = val1.value;
                        docBuilder.field(String.valueOf(storedField), val);
                        docBuilder.field(storedField + "_search", val
                                .toLowerCase());
                    }
                }
            }

            if (dspaceObject instanceof Bitstream) {
                Bitstream bit = (Bitstream) dspaceObject;
                Bundle[] bundles = bit.getBundles();
                docBuilder.field("bundleName").startArray();
                for (Bundle bundle : bundles) {
                    docBuilder.value(bundle.getName());
                }
                docBuilder.endArray();
            }


            storeParents(docBuilder, getParents(dspaceObject));


            log.info(docBuilder.toString());
            docBuilder.endObject();

            if (docBuilder != null) {
                IndexRequestBuilder irb = client.prepareIndex(indexName, indexType)
                        .setSource(docBuilder);
                irb.execute().actionGet();
            }

            client.close();

        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {

            log.error(e.getMessage(), e);
        }
    }


    public static void buildParents(DSpaceObject dso, HashMap<String, ArrayList<Integer>> parents)
            throws SQLException {
        if (dso instanceof Community) {
            Community comm = (Community) dso;
            while (comm != null && comm.getParentCommunity() != null) {
                comm = comm.getParentCommunity();
                parents.get("owningComm").add(comm.getID());
            }
        } else if (dso instanceof Collection) {
            Collection coll = (Collection) dso;
            for (Community community : coll.getCommunities()) {
                parents.get("owningComm").add(community.getID());
                buildParents(community, parents);
            }
        } else if (dso instanceof Item) {
            Item item = (Item) dso;
            for (Collection collection : item.getCollections()) {
                parents.get("owningColl").add(collection.getID());
                buildParents(collection, parents);
            }
        } else if (dso instanceof Bitstream) {
            Bitstream bitstream = (Bitstream) dso;

            for (Bundle bundle : bitstream.getBundles()) {
                for (Item item : bundle.getItems()) {

                    parents.get("owningItem").add(item.getID());
                    buildParents(item, parents);
                }
            }
        }

    }

    public static HashMap<String, ArrayList<Integer>> getParents(DSpaceObject dso)
            throws SQLException {
        HashMap<String, ArrayList<Integer>> parents = new HashMap<String, ArrayList<Integer>>();
        parents.put("owningComm", new ArrayList<Integer>());
        parents.put("owningColl", new ArrayList<Integer>());
        parents.put("owningItem", new ArrayList<Integer>());

        buildParents(dso, parents);
        return parents;
    }

    public static void storeParents(XContentBuilder docBuilder, HashMap<String, ArrayList<Integer>> parents) throws IOException {

        Iterator it = parents.keySet().iterator();
        while (it.hasNext()) {

            String key = (String) it.next();

            ArrayList<Integer> ids = parents.get(key);

            if (ids.size() > 0) {
                docBuilder.field(key).startArray();
                for (Integer i : ids) {
                    docBuilder.value(i);
                }
                docBuilder.endArray();
            }
        }
    }


    public static boolean isUseProxies() {
        return useProxies;
    }

}
