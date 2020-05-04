# -*- coding: utf-8 -*-
"""
Created on Wed Apr 22 10:26:44 2020

@author: cody.schiffer
"""

import pandas as pd
import os
from rdflib.graph import Graph
import gzip

from io import StringIO

os.chdir(r"C:\Users\cody.schiffer\Documents\creating_mesh_tables")
base_directory = os.getcwd()




#nt_filename = '\mesh2020.nt'
#g = Graph()
#q = g.parse(base_directory + nt_filename, format="nt")


from py7zr import unpack_7zarchive
import shutil

shutil.register_unpack_format('7zip', ['.7z'], unpack_7zarchive)
shutil.unpack_archive(base_directory + '\RRF_files.7z', base_directory)


desc_path= base_directory + '\MRSAT.RRF'
desc_column_ids=[0,5,10]
desc_column_names=[ 'concept_id', 'id', 'tree_no']


con_path= base_directory + '\MRCONSO.RRF'
con_column_ids=[0,6,7,14]
con_column_names= 'concept_id', 'preferred_term?', 'alias_id', 'alias'

meshmoas_syns_file='meshmoas_syns.csv'
meshdis_syns_file='meshdis_syns.csv'
pubmed_abs_path= '../resource/28.tar.gz'

pubmed_MeSH_tags_file='pubmed_MeSH_tags.txt'


def load_large_csv(path,func,*func_args, header=None, chunksize=1000000, **func_options):
    #path: file location
    #func: function that process chunk
    #func_args, func_options: optional arguments for func
    
    df=[]
    for chunk in pd.read_csv(path, chunksize=chunksize, delimiter='|',header=None):

        df.append(func(chunk,*func_args,**func_options))
    df=pd.concat(df)
    return df

#mrsat_rrf_path = base_directory + '\MRSAT.RRF'
#load MRSAT.RRF 
# unzip 7z file: 7z x RRF_files.7z 
    

##Lets load in the preferred alias data
df_con=load_large_csv(con_path,lambda chunk: chunk.iloc[:,con_column_ids])
df_con.columns=con_column_names

##For diseases only
df_disease=load_large_csv(desc_path,lambda chunk: chunk[chunk.iloc[:,9].fillna('').str.contains("MSH") & chunk.iloc[:,8].fillna('').str.contains("MN") & chunk.iloc[:,10].fillna('').str.contains("C")].iloc[:,desc_column_ids])
df_disease.columns=desc_column_names
#inner join df_diseasev with df_con
meshdis_syns=pd.merge(df_disease,df_con,on=['concept_id'],how='inner')
#save to csv
meshdis_syns.to_csv(base_directory + '\/'+meshdis_syns_file,sep='|')


#bin_filename = '\d2020.bin'
#counter = 0
#with open(base_directory + bin_filename, mode='rb') as file: # b is important -> binary
#    bytes_data = file.read()
#    #for line in file:
#    #    counter += 1
#    #    
#    #   print(l
#s=str(bytes_data,'utf-8')
#
#data = StringIO(s) 
#
#df=pd.read_csv(data)   
#sample = pd.DataFrame(fileContent)