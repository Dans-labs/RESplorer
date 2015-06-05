from pymongo.mongo_client import MongoClient
from codes import STAT_SECOND_HAND, STAT_PUB_CHAIN, STAT_SOURCES, \
    STAT_TRIPLES, DESC_TYPE

import logging
import math
from resplorer.codes import DESC_URI
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
    
    # Count the number of triples from the source and copied from another
    triples = {'all':0, 'second':0}
    
    # Number of instances per type
    instances = {}
    
    # Number of instances per data source
    sources = {}
    
    # Number of instances per profile
    profiles = {}
    
    count = 0
    for post in db.posts.find():
        count = count + 1
        chains_cleaned = [chain for chain in post[STAT_PUB_CHAIN] if not chain.startswith('233a')]
        profile = (post[DESC_TYPE], " # ".join(sorted(chains_cleaned)))
        profiles.setdefault(profile, 0)
        profiles[profile] = profiles[profile] + 1
        for chain in post[STAT_PUB_CHAIN]:
            chains.setdefault(chain, 0)
            chains[chain] = chains[chain] + 1
        for source in post[STAT_SOURCES]:
            sources.setdefault(source, 0)
            sources[source] = sources[source] + 1
        triples['all'] = triples['all'] + post[STAT_TRIPLES]
        triples['second'] = triples['second'] + post[STAT_SECOND_HAND]
        instances.setdefault(post[DESC_TYPE], 0)
        instances[post[DESC_TYPE]] = instances[post[DESC_TYPE]] + 1
        
    # Print the results
    print "Total number of entities :\n\t{}".format(count)
    print "Total number of triples :\n\t{}".format(triples)
    print "Data import chains :"
    for chain in chains:
        if not chain.startswith('233a'):
            print '\t' + chain
    print "Instances per type :"
    for (k, v) in instances.iteritems():
        print "\t{1} : {0}".format(k, v)
    print "Instances per source :"
    for (k, v) in sources.iteritems():
        print "\t{1} : {0}".format(k, v)

    # Track down weird hostnames
    with open('weird.txt', 'w') as output_file:
        for post in db.posts.find():
            weird = False
            for chain in post[STAT_PUB_CHAIN]:
                weird = weird or chain.startswith('233a')
            if weird:
                output_file.write("{}\n".format(post[DESC_URI]))
           
    
    # Prepare the gdl file
    colors = []
    with open('graph.gdl', 'w') as output_file:
        output_file.write("graph resnetwork {\n")
        output_file.write("\tlabelloc=t;\n")
        output_file.write("\tlabel=\"Resources similarity in RES\";\n")
        # Find the biggest count
        max_v = 0
        for (k, v) in profiles.iteritems():
            if math.log(v) > max_v: max_v = math.log(v)
        # Write all the nodes
        node_index = 0
        for (k, v) in profiles.iteritems():
            (class_name, chains) = k
            if class_name not in colors: colors.append(class_name)
            color = colors.index(class_name) + 1 
            size = 1+3 * (math.log(v) / (max_v * 1.0))
            line = "\tnode{} [style=filled,fillcolor=\"/accent8/{}\",shape=circle,label=\"\",fixedsize=true,width={}];\n".format(node_index, color, size)
            output_file.write(line)
            node_index = node_index + 1
        # Write all the connections
        nodes = profiles.items()
        for first_index in range(0, len(nodes)):
            for second_index in range(first_index + 1, len(nodes)):
                (first_type, first_chains), first_cnt = nodes[first_index]
                (second_type, second_chains), second_cnt = nodes[second_index]
                first_set = set(first_chains.split(' # '))
                second_set = set(second_chains.split(' # '))
                jaccard = (1.0*len(first_set.intersection(second_set))) / len(first_set.union(second_set))
                if jaccard > 0:
                    line="\tnode{} -- node{} [penwidth={}];\n".format(first_index, second_index, 5*jaccard)
                    output_file.write(line)
        # Write a legend
        output_file.write("\tsubgraph legend {\n")
        output_file.write("\t\trank=\"sink\";\n")
        output_file.write("\t\tlabel = \"Legend\";\n")
        for key_index in range(0, len(colors)):
            class_name = colors[key_index]
            line = "\t\tkey{} [style=filled,fillcolor=\"/accent8/{}\",label=\"{}\"];\n".format(key_index, key_index + 1, class_name)
            output_file.write(line)
            key_index = key_index + 1
        output_file.write("\t}\n")
        
        # Close
        output_file.write("}\n")