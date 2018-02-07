#!/usr/bin/env python

import sys
import os

from Document import Document
from Cluster import Cluster
import MathUtil
import HDFSUtil

import math


class KmeansMapper:
    def __init__(self):
        self.K = int(os.environ.get("numClusters", 1))
        self.iteration = int(os.environ.get("kmeansIter", 1))
        self.kmeans_hdfs_path = os.environ.get("kmeansHDFS")
        self.hadoop_prefix = os.environ.get("hadoopPrefix")
        self.clusters = []


    
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

    def read_input(self, file):
        for line in file:
            yield line.rstrip()

    def emit(self, key, value, separator="\t"):
        """ Emit (key, value) pair to stdout for hadoop streaming """
        print >> sys.stderr, "emitting document: %s" % key
        print '%s%s%s' % (key, separator, value)

    def main(self):
        if self.iteration == 1:
            path = self.kmeans_hdfs_path + "/cluster0/cluster0.txt"
        else:
            path = self.kmeans_hdfs_path + "/output/cluster" + str(self.iteration - 1) + "/part-00000"
        for line in HDFSUtil.read_lines(path, hadoop_prefix=self.hadoop_prefix):
            if self.iteration > 1:
                if line.startswith("cluster"):
                    line = line.split("\t", 1)[1]
                else:
                    continue
            c = Cluster()
            c.read(line)
            self.clusters.append(c)
        data = self.read_input(sys.stdin)
        for line in data:
            
            self.map(line)

    '''
    emit nearest centers (key) and points (value).
    emit only uid
    self.tfidf[int(pairs[0])] = float(pairs[1])
    self.uid = splits[0]
    '''
    def map(self, line):
        # TODO: Your code goes here -- call `self.emit(key, value)`
        
        doc = Document(line)
        shortest = float('inf')        
        # current cluster lable of this data point
        cur_center = 999
        for cluster in self.clusters:
            dist_temp=self.l2_norm(doc.tfidf, cluster.tfidf)

            if dist_temp<shortest:
                shortest = dist_temp
                cur_center = cluster.uid
 
        self.emit(str(cur_center),str(doc))
        # pass


if __name__ == '__main__':
    mapper = KmeansMapper()
    mapper.main()
