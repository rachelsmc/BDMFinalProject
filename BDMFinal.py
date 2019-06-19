from pyspark import SparkContext

def cleaningDrugList(txtFile):
    newDrugList = []
    textFile = open(txtFile, "r")
    drugList = textFile.read().split("\n")
    for drug in drugList:
        drugRevised = " {0} ".format(drug)
        newDrugList.append(drugRevised)
    return newDrugList

def deEmojify(inputString):
    return inputString.encode('ascii', 'ignore').decode('ascii')

def cleaningTweet(string):
    import re
    stringLower = string.lower()
    cleanedString = re.sub('[^0-9a-zA-Z]+', ' ', stringLower)
    cleanedStringRevised = " {0} ".format(cleanedString)
    return cleanedStringRevised

def createIndex(geojson):
    import rtree
    import fiona.crs
    import geopandas as gpd
    zones = gpd.read_file(geojson)
    index = rtree.Rtree()
    for idx,geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)

def findTract(p, index, zones):
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        if zones.geometry[idx].contains(p):
            return idx
    return None

def tweetFilter(pid, records):
    illegal = cleaningDrugList("drug_illegal.txt")
    sched = cleaningDrugList("drug_sched2.txt")
    drugsList = illegal+sched
    indexT, tractsT = createIndex('500cities_tracts.geojson')
    tractName = tractsT['plctract10']
    tractPop = tractsT['plctrpop10']
    import re
    for line in records:
        row = line.split('|')
        if len(row) != 7:
            continue
        else:
            cleanedTweet = cleaningTweet(row[5])
            words_re = re.compile("|".join(drugsList))
            if words_re.search(cleanedTweet):
                import shapely.geometry as geom
                try:
                    lon = float(row[2])
                except:
                    continue
                try:
                    lat = float(row[1])
                except:
                    continue
                tract = None
                try:
                    loc = geom.Point((float(lon), float(lat)))
                except:
                    continue
                try:
                    tract = findTract(loc, indexT, tractsT)
                except:
                    continue
                if tract is not None:
                    if int(tractPop[tract])>0:
                        yield ((tractName[tract], tractPop[tract]), 1)

def main(sc):
	import sys
	rdd = sc.textFile(sys.argv[1])
	normalisedCounts = rdd.mapPartitionsWithIndex(tweetFilter) \
				.filter(lambda x: x is not None) \
				.reduceByKey(lambda a,b: a+b) \
				.map(lambda x: (x[0][0], x[1]/x[0][1])) \
				.sortByKey(ascending=True) \
				.collect()
	print(normalisedCounts)

if __name__ == "__main__":
    sc = SparkContext()
    main(sc)
