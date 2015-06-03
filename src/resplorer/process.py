from pymongo.mongo_client import MongoClient
from resplorer.codes import STAT_SECOND_HAND, STAT_PUB_CHAIN, STAT_NB_SOURCES, \
    STAT_TRIPLES, DESC_URI, DESC_TYPE

import logging
logger = logging.getLogger(__name__)

if __name__ == '__main__':
    # Logging
    root_logger = logging.getLogger('')
    root_logger.setLevel(logging.INFO)
    logFormat = '%(asctime)s %(name)-14s %(levelname)-6s %(message)s'
        
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter(logFormat))
    root_logger.addHandler(ch)
    
    fh = logging.FileHandler('process.log', mode='w')
    fh.setFormatter(logging.Formatter(logFormat))
    root_logger.addHandler(fh)

    # DB    
    client = MongoClient()
    db = client['RESplorer']

    # Aggregate data about chains
    chains = {}
    triples = {'all':0, 'second':0}
    instances = {}
    count = 0
    for post in db.posts.find():
        count = count + 1
        for chain in post[STAT_PUB_CHAIN]:
            chains.setdefault(chain, 0)
            chains[chain] = chains[chain] + 1
        triples['all'] = triples['all'] + post[STAT_TRIPLES]
        triples['second'] = triples['second'] + post[STAT_SECOND_HAND]
        instances.setdefault(post[DESC_TYPE], 0)
        instances[post[DESC_TYPE]] = instances[post[DESC_TYPE]] + 1
        
    # Print the results
    print "Total number of entities :\n\t{}".format(count)
    print "Total number of triples :\n\t{}".format(triples)
    print "Data import chains :\n\t{}".format(chains)
    print "Instances per type :"
    for (k,v) in instances.iteritems():
        print "\t{1} : {0}".format(k,v)
