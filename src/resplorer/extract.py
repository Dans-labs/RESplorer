'''
Created on 2 Jun 2015

@author: cgueret
'''
import requests
import rdflib
import StringIO
import urlparse
import re
import multiprocessing
import signal
from pymongo.mongo_client import MongoClient
from codes import STAT_SECOND_HAND, STAT_PUB_CHAIN, STAT_NB_SOURCES,\
    STAT_TRIPLES, DESC_URI, DESC_TYPE

import logging
logger = logging.getLogger(__name__)

ROOT_API = 'http://acropolis.org.uk/'

CLASSES = {'Agent'       : 'http://xmlns.com/foaf/spec/#term_Agent',
           'Collection'  : 'http://purl.org/dc/dcmitype/Collection',
           'Concept'     : 'http://www.w3.org/TR/skos-reference/#concepts',
           'Work'        : 'http://vocab.org/frbr/core.html#Work',
           'Dataset'     : 'http://rdfs.org/ns/void#Dataset',
           'Document'    : 'http://xmlns.com/foaf/0.1/Document',
           'Event'       : 'http://motools.sourceforge.net/event/event.html#term_Event',
           'Organization': 'http://xmlns.com/foaf/spec/#term_Organization',
           'People'      : 'http://xmlns.com/foaf/spec/#term_Person',
           'Physical thing': 'http://www.cidoc-crm.org/cidoc-crm/E18_Physical_Thing',
           'Location'    : 'http://www.w3.org/2003/01/geo/',
           '/events'     : 'http://purl.org/NET/c4dm/event.owl#Event',
           '/people'     : 'http://xmlns.com/foaf/0.1/Person',
           '/places'     : 'http://www.w3.org/2003/01/geo/wgs84_pos#SpatialThing',
           '/concepts'   : 'http://www.w3.org/2004/02/skos/core#Concept',
           '/groups'     : 'http://xmlns.com/foaf/0.1/Group',
           '/works'      : 'http://purl.org/vocab/frbr/core#Work',
           '/agents'     : 'http://xmlns.com/foaf/0.1/Agent'
           }

#CLASSES = {'Agent'       : 'http://xmlns.com/foaf/spec/#term_Agent',
#           '/works'      : 'http://purl.org/vocab/frbr/core#Work',
#           }

def get_nq_locations(class_name):
    session = requests.Session()
    
    logger.info('Fetching a list of {}'.format(class_name))
        
    nq_locations = []

    offset=0
    while offset != None:
        # Get
        params = {'class': class_name, 'offset' : offset}
        headers = {'Accept': 'text/turtle'}
        req = session.get(ROOT_API, params=params, headers=headers)
        
        # Parse
        offset = None # Reset the offset for the next loop (if any)
        for line in req.iter_lines():
            m = re.match('^<(http://acropolis.org.uk/[0-9a-z]{32})#id>$', line)
            if m != None:
                nq_locations.append(m.group(1) + '.nq')
                continue
            m = re.search('xhtml:next <\/\?offset=(\d*)', line)
            if m != None:
                offset = m.group(1)
                continue
        req.close()
    
    session.close()
        
    return nq_locations

def parse_document(location):
    stats = {STAT_SECOND_HAND : 0,
             STAT_NB_SOURCES : 0,
             STAT_PUB_CHAIN : [],
             STAT_TRIPLES : 0,
             DESC_URI : location}
    
    try:
        req = requests.get(location)
        graph = rdflib.ConjunctiveGraph()
        graph.parse(StringIO.StringIO(req.text), format='nquads')
        req.close()
        
        contexts = [context for context in graph.contexts()]
        stats[STAT_NB_SOURCES] = len(contexts)
        for context in contexts:
            source = urlparse.urlparse(context.identifier).hostname
            if source == 'acropolis.org.uk':
                continue
            for subj, _, _ in graph.triples((None, None, None), context):
                stats[STAT_TRIPLES] = stats[STAT_TRIPLES] + 1
                authority = urlparse.urlparse(subj).hostname
                if authority != source:
                    stats[STAT_SECOND_HAND] = stats[STAT_SECOND_HAND] + 1
                    chain = '{} -> {}'.format(authority, source)
                    if chain not in stats[STAT_PUB_CHAIN]:
                        stats[STAT_PUB_CHAIN].append(chain)
    except Exception as details:
        logger.error('Error processing {}'.format(location))
        logger.error(details)
    
    return stats
    
#'http://acropolis.org.uk/921df22045f949c5967a4d5e9dd0e653.nq'

def init_worker():
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    
def process_classes(db, class_uri):
    # Get all the target locations
    locations = get_nq_locations(class_uri)
    logger.info('Got {} URIs to process'.format(len(locations)))
    
    if len(locations) == 0:
        return
    
    # Get all the data
    results = []
    pool = multiprocessing.Pool(40, init_worker)
    try:
        results = pool.map(parse_document, locations)
        pool.close()
        pool.join()
    except KeyboardInterrupt:
        pool.terminate()
        pool.join()
    
    # Add a bit more extra information
    for result in results:
        result[DESC_URI] = result[DESC_URI].replace('.nq', '#id')
        result[DESC_TYPE] = class_uri
    
    # Save all that in the DB
    logger.info('Store {} results in the DB'.format(len(results)))
    db.posts.insert_many(results)
    
if __name__ == '__main__':
    return

    root_logger = logging.getLogger('')
    root_logger.setLevel(logging.INFO)
    logFormat = '%(asctime)s %(name)-10s %(levelname)-6s %(message)s'
        
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter(logFormat))
    root_logger.addHandler(ch)
    
    fh = logging.FileHandler('extract.log', mode='w')
    fh.setFormatter(logging.Formatter(logFormat))
    root_logger.addHandler(fh)
    
    logging.getLogger("requests").setLevel(logging.WARNING)
    
    client = MongoClient()
    client.drop_database('RESplorer')
    db = client['RESplorer']
    
    for (name, uri) in CLASSES.iteritems():
        logger.info('Processing {}'.format(name))
        process_classes(db, uri)
        