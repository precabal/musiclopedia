import sys, json, time
from dateutil.parser import *

import musicbrainzngs as m


def main(*argv):
	m.set_useragent("applicatiason", "0.201", "http://recabal.com")
	m.set_hostname("localhost:8080")
	m.auth("vm", "musicbrainz")
	
	f = open(sys.argv[1],'r')
	for line in f.xreadlines():
		
		line = line.strip();
		lifeSpanString = "false,false"

		art_dict = m.search_artists(artist=line,limit=1,strict=True,tag="jazz")
		
		if art_dict['artist-count']!=0:
			lifeSpanString = getLifeSpan(art_dict)
			
		else:
			art_dict = m.search_artists(artist=line,limit=1,strict=True)
			if art_dict['artist-count']!=0:
				lifeSpanString = getLifeSpan(art_dict)			

		print line+","+lifeSpanString
				
	f.close()
	
def getLifeSpan(artistDictionary):
	life_span = artistDictionary['artist-list'][0]['life-span']
	
	#check if the field 'begin' exists in the life_span returned dictionary
	if 'begin' in life_span:
		startDate = life_span['begin']
	else:
		startDate = "false"

	if 'ended' in life_span:
		endDate = life_span['ended']
	else:
		endDate = "false"
	
	#if we retrieved a date, parse it and obtain the year
	if startDate!="false":
		startDate = str(parse(startDate).year)

	return startDate+","+endDate



if __name__ == "__main__":
	main()




