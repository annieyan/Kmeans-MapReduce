from __future__ import division
import numpy as np
import os
from collections import Counter
import nltk
from nltk import word_tokenize
from nltk.util import ngrams
import string
from collections import deque
from itertools import islice
import collections
import math
import argparse
import time
import json
from sklearn.metrics import accuracy_score
import matplotlib.pyplot as plt
from sklearn.metrics import confusion_matrix
import re
import matplotlib.pyplot as plt
import itertools
import sys
from numpy import random


'''
@ doc: dict(termid1:tfidf1,termid2:tfidf2)
'''
def l2_norm(doc1,doc2):
    l2_norm = 0.0
    # dim = len(vec2)
    key_set1 = set(doc1.keys())
    key_set2 = set(doc1.keys())
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
vector representation for a document using tfidf
tfidf_dict: {termid: tfidf value})
return tuple {vector[V_size]}
'''
def doc_to_vec(documents,tfidf_dict):
    dim= len(tfidf_dict)
    doc_vec = list()
    doc_count = len(documents)
    docid_list = documents.keys()
    for docid in range(doc_count):
        temp_list = tuple()
        for termid in range(dim):
            # termid starts from 1
            if tfidf_dict[termid+1].has_key(docid+1):
                temp_list = temp_list+(tfidf_dict[termid+1][docid+1],)
            else:
                temp_list = temp_list+(0.0,)
        doc_vec.append(temp_list)
    return doc_vec


'''
given a cluster of points, return the center point
tfidf[int(pairs[0])] = float(pairs[1])

'''
def cluster_center(point_set):
    return None
    

'''
the sum distance of every point to its cluster center
'''
def norm_to_center(point_set, center):
    dist = 0.0
    for point in point_set:
        # skip label in point
        # point_temp = point[1:]
        dist+= l2_norm(point,center)

    return dist

'''
given a set of J, get summary stat
'''
def sum_stat(result_set):
    result_list = list(result_set)
    min_J = min(result_list)
    max_J = max(result_list)
    mean_J = np.mean(result_list)
    sd_J = np.std(result_list)
    return min_J, max_J,mean_J, sd_J


'''
given a dict of points with its shortest distance
used in Kmeans++
input : dict {point[x1,x2], l2_norm}
'''
def random_pick_prob(point_dict):
    point_list = list()
    dist_list = list()
    index_list = range(0,len(point_dict))
    for point,dist in point_dict.iteritems():
        point_list.append(point)
        dist_list.append(dist)
    # get probabilities
    norm = [float(i)/float(sum(dist_list)) for i in dist_list]
    # print("----- point_list---",point_list)
    chosen_index = random.choice(index_list, 1, replace=False, p=norm)
    print("chosen index",chosen_index[0])
    chosen_center = point_list[chosen_index[0]]
    print("pick next point:",chosen_center)
    return chosen_center


'''
given a set of points : set of tuples or list of tuples, 
and a dimension, 
return a n*dim array of points 
'''
def points_to_array(point_set,dim):
    n = len(point_set)
    point_array = np.empty([n,dim])
    i = 0
    if dim ==2:
        for point in point_set:
            # print("point",point)
            point_array[i,0] =  point[1]
            point_array[i,1] =  point[2]
            i = i+1
    else:
        for point in point_set:
            # print("point",point)
            for d in range(dim):
                point_array[i,d]=point[d]
            i = i+1

    # for pretty print np array    
    # np.set_printoptions(precision=3)
    # print("point array",point_array)
    return point_array