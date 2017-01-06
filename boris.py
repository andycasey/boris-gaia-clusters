
import yaml
from astropy.table import Table
from database import Database as WSDB


# Load clusters.
clusters = Table.read("clusters.txt", format="ascii")


# Credentials
with open("wsdb.yaml", "r") as fp:
    credentials = yaml.load(fp)

wsdb = WSDB(**credentials)

cone = 1.0/3600 # degrees
catalog_names = ("apassdr9.main", "twomass.psc", "unwise.sdss_forced")

for cluster_name, ra, dec, radius, N in clusters:

    print("Querying TGAS for {}".format(cluster_name))

    kwds = dict(cluster_ra=ra, cluster_dec=dec, radius=radius, cone=cone)

    # Query all TGAS within +/- radius.
    tgas_sources = wsdb.retrieve_table(
        """ SELECT *
            FROM gaia_dr1.tgas_source
            WHERE q3c_radial_query(ra, dec, %(cluster_ra)s, %(cluster_dec)s, %(radius)s)
        """, kwds)
    tgas_sources.write("{}-tgas.csv".format(cluster_name))

    print("Found {N} TGAS sources for {cluster_name}".format(
        N=len(tgas_sources), cluster_name=cluster_name))

    # Cross-match TGAS with catalogs in the same radius.
    for catalog_name in catalog_names:

        print("Cross-matching TGAS with {} for {}".format(catalog_name, cluster_name))
        
        catalog_short_name = catalog_name.split(".")[0]
        
        tgas_xmatch_sources = wsdb.retrieve_table(
            """ WITH tgas AS (
                    SELECT *
                    FROM gaia_dr1.tgas_source
                    WHERE q3c_radial_query(ra, dec, %(cluster_ra)s, %(cluster_dec)s, %(radius)s)
                    )
                SELECT * FROM tgas, {catalog_name} AS {catalog_short_name}
                WHERE q3c_join(
                    tgas.ra, tgas.dec, 
                    {catalog_short_name}.ra, {catalog_short_name}.{catalog_dec}, 
                    %(cone)s)
            """.format(
                catalog_name=catalog_name,
                catalog_short_name=catalog_short_name,
                catalog_dec="decl" if catalog_short_name == "twomass" else "dec"),
            kwds)

        tgas_xmatch_sources.write("{cluster_name}-tgas-{catalog_short_name}.csv"\
            .format(cluster_name=cluster_name,
                    catalog_short_name=catalog_short_name))

        print("Found {N} TGAS-{catalog_short_name} sources for {cluster_name}"\
            .format(N=len(tgas_xmatch_sources), 
                    catalog_short_name=catalog_short_name,
                    cluster_name=cluster_name))
