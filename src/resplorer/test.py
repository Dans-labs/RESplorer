import requests
import rdflib
import StringIO

ROOT_API = 'http://acropolis.org.uk/'
class_name = 'http://www.w3.org/2004/02/skos/core#Concept'
offset = '0'

params = {'class': class_name, 'offset' : offset}
headers = {'Accept': 'text/turtle'}
r = requests.get(ROOT_API, params=params, headers=headers)
data = r.text

print data

graph = rdflib.Graph()
graph.parse(StringIO.StringIO(data), format='turtle')
