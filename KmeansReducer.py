#!/usr/bin/env python

import sys
import math
from itertools import groupby
from operator import itemgetter

from Document import Document
from Cluster import Cluster
import MathUtil




class KmeansReducer:
    """ Update the cluster center and compute the with-in class distances """

    def emit(self, key, value, separator="\t"):
        """ Emit (key, value) pair to stdout for hadoop streaming """
        print '%s%s%s' % (key, separator, value)

    def read_mapper_output(self, file, separator='\t'):
        for line in file:
            yield line.rstrip().split(separator, 1)


        '''
    @ doc: dict(termid1:tfidf1,termid2:tfidf2)
    '''
    def l2_norm(self,doc1,doc2):
        l2_norm = 0.0
        # dim = len(vec2)
        key_set1 = set(doc1.keys())
        key_set2 = set(doc2.keys())
        intersect = set(key_set1).intersection(key_set2)
        unique_keys1 = key_set1-intersect
        unique_keys2 = key_set2- intersect
        for termid in unique_keys1:
            l2_norm += math.pow(doc1[termid],2)
        for termid in unique_keys2:
            l2_norm += math.pow(doc2[termid],2)
        for termid in intersect:
            l2_norm += math.pow(doc2[termid]-doc1[termid],2)

        return l2_norm

    '''
    emit new centers
    values are lines
    '''
    def reduce(self, uid, values):
        c = Cluster()
        sqdist = 0.0
        point_count = len(values)
        center_dict = dict()
        c.uid = uid

        # TODO: Put your code here!
        # recompute new centers
        for value in values:
            doc = Document(value)
           
            tfidf_dict = doc.tfidf
            for termid,val in tfidf_dict.iteritems():
                center_dict[termid] = center_dict.get(termid,val)+val

        for termid, val in center_dict.iteritems():
            center_dict[termid] = float(center_dict[termid])/point_count
        
        for value in values:
            doc = Document(value)
           
            tfidf_dict = doc.tfidf
            sqdist = sqdist+ self.l2_norm(tfidf_dict,center_dict)

        c.tfidf = center_dict
        # plot square distance


        # Output the cluster center into file: clusteri
        self.emit("cluster" + str(c.uid), str(c))
        # Output the within distance into file: distancei
        self.emit("distance" + str(c.uid), str(c.uid) + "|" + str(sqdist))






    def main(self):
        data = self.read_mapper_output(sys.stdin)
        
        # combiner
        # itemgetter(0) is the group key, which is cluster center
        for uid, values in groupby(data, itemgetter(0)):
            vals = list()
            # values is key, value: uid, val, which is a groupby object.
            # val is a bunch of points represented in str(Document) 
            # vals = [val[1] for val in values]
            for val in values:
                print >> sys.stderr, "reduce val: %s" % str(val)
                # form a set of points in str(Document) format
                vals.append(val[1])            
            # center uid, points belong to clusters
            print("-------------begin reduce---------------")
            # input all tfidf_dict
            self.reduce(uid, vals)


if __name__ == '__main__':
    reducer = KmeansReducer()
    reducer.main()
