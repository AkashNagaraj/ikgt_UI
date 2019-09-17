import socket
from collections import Counter
from flask import Flask
from flask import render_template, url_for, redirect, request, jsonify, make_response
import logging
import re
from elasticsearch import Elasticsearch

app = Flask(__name__)
logger = logging.getLogger(__name__)
Client = Elasticsearch(['10.24.28.112'],scheme='http',port=9200,sniff_on_start=True,sniff_on_connection_fail=True, sniffer_timeout=6000,timeout=1200,max_retries=10,retry_on_timeout=True,http_auth=('aritra','aritra'))

def get_unique_ent():
   with open('extractions.txt') as f:
     Ent_list = f.readlines()
     Ent1_list = list(set(['_'.join(x.strip().split(',')[1].title().split()) for x in Ent_list]))
     Ent2_list = list(set(['_'.join(x.strip().split(',')[0].title().split()) for x in Ent_list]))
   return(Ent1_list,Ent2_list)

def get_sentence_ids(client,ent1,ent2):
   ent1=' '.join(ent1.strip().split('_'))   
   ent2=' '.join(ent2.strip().split('_'))
   response=client.search(index="np_pair_bags_with_ner",body={
      "query": {
      "bool": {
      "must": [
        {"match_phrase":{"doc.entities.name": ent1}},
        {"match_phrase":{"doc.entities.name": ent2}}
      ]
    }}
    })
   hits=response['hits']['hits']
 
   index_sent=[] 
   for hit in hits:
        for ids in hit['_source']['doc']['sentences']:
           for k,v in ids.items():
               index_sent.append(get_sentences(client,v))
   return(index_sent)

def get_sentences(client,id_value):
    response=client.search(index="inell_clean_preproc",body=
    {
       "_source": ["doc.sent"],
       "query": 
       {
            "match": {"doc.id": id_value}
       }
    }
    )
    hits=response['hits']['hits']

    for hit in hits:
        return(hit['_source']['doc'])

def get_relation(ent1,ent2):
    with open('extractions.txt') as f:
        entities = f.readlines()
        count=0
        entities = [x.strip() for x in entities]
        for x in entities:   
            ent1='_'.join(ent1.strip().title().split())
            ent2='_'.join(ent2.strip().title().split())
            e1,e2,_=x.split(',')
            e1='_'.join(e1.strip().title().split())
            e2='_'.join(e2.strip().title().split())           
            
            if(e1==ent2 and e2==ent1):
               count+=1

        if(count==0):
          return("False")
        else:
          return("True")

@app.route('/')
def hello():
    entities1 , entities2 = get_unique_ent()
    return render_template('index.html', data={},entities1=entities1,entities2=entities2)

@app.route('/search', methods=['POST'])
def search():
    client = Client  
    entities1 , entities2 = get_unique_ent()
    ent1 = request.form.get('entity_1')
    ent2 = request.form.get('entity_2')
    
    norm_ent1,norm_ent2=[],[]
    result={}
    result['predicted_relation']=get_relation(ent1,ent2)
    result['sentences']=get_sentence_ids(client,ent1,ent2)
    result['sub']=ent1   
    result['obj']=ent2 
    result['bag_size']=len(result['sentences'])    
    return render_template("bags.html", data={'result': result},entities1=entities1,entities2=entities2)

@app.route('/process',methods=['POST'])
def process():
     query=request.form.get('sentence')
     Ent=query.split("AND")
     Ent=[x.replace('"','').strip() for x in Ent]
     client = Client
     entities1 , entities2 = get_unique_ent() 
     must_match=[]
     for ent in Ent:
        must_match.append({"match_phrase": {"doc.sent": str(ent)}})
     response=client.search(index="inell_clean_preproc",body={
        "size":10000,
        "_source": "doc.sent",
       "query": {
       "bool": { "must": must_match }
          }
       })
  
     hits=response['hits']['hits']
     sentence=[]
     result={}
     for hit in hits:
       sentence.append(hit['_source']['doc']['sent'])        
     result['sentences']=list(set(sentence))#sentence
     result['num']=len(sentence)
     result['query']=query
     return render_template('result.html',data={'result':result},entities1=entities1,entities2=entities2)

if __name__ == '__main__':
    app.config['TEMPLATES_AUTO_RELOAD'] = True
    sock=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('localhost',0))
    port=sock.getsockname()[1]
    sock.close()
    app.run(host='10.24.28.102', port=port,debug=True)
