[out:json][timeout:90][maxsize:33554432];
(
 nwr["amenity"~"(^|;)[ ]*(restaurant|cafe|fast_food|pub|bar)[ ]*(;|$)"]({{bbox}});
 nwr["shop"~"(^|;)[ ]*bakery[ ]*(;|$)"]({{bbox}});
 nwr["fhrs:id"]({{bbox}});
);
out meta geom;
out count;
