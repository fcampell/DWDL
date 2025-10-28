import geopandas as gpd

db = gpd.read_file('solarenergie-eignung/fassaden_zürich.gpkg')#, layer="SOLKAT_CH_FASS")


print(db.columns)
#print(df.)