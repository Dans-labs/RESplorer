import requests
import re


ROOT_API = 'http://acropolis.org.uk/'
class_name = 'http://www.w3.org/2003/01/geo/wgs84_pos#SpatialThing'
offset = '100050'

params = {'class': class_name, 'offset' : offset}
headers = {'Accept': 'text/turtle'}
r = requests.get(ROOT_API, params=params, headers=headers)
data = r.text

for line in data.split('\n'):
    m = re.match('^<(http://acropolis.org.uk/[0-9a-z]{32})#id>$', line)
    if m != None:
        pass
        #print m.group(1)
    m = re.search('xhtml:next <\/\?offset=(\d*)', line)
    if m != None:
        print line
        print m.group(1)
    