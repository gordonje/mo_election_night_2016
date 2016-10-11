topojson > topojson/mo_house_2000.topo.json \
-q 5000 \
-s 0.00000008 \
    house_districts=state_legislative/2000sl_shp/sl29_d00.shp --id-property GEOID 

topojson > topojson/mo_senate_2000.topo.json \
-q 5000 \
-s 0.00000008 \
    house_districts=state_legislative/2000su_shp/su29_d00.shp --id-property GEOID 