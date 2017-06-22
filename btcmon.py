# To change this license header, choose License Headers in Project Properties.
# To change this template file, choose Tools | Templates
# and open the template in the editor.

__author__ = "S4G4"
__date__ = "$Sep 25, 2016 6:57:39 AM$"

import sys
import json, requests
import time
ES_URL = "http://localhost:9200/btcmon"

TRANSACTION_TABLE  = ES_URL+"/transactions"
BTC_ADDR_TABLE  = ES_URL+"/btcaddr"

'''
mapping for the transactions - (transactions)
{
  "properties": {
    "btc_address": {
      "type": "string",
      "index": "not_analyzed"
    },
    "date": {
      "type":"date"
    },
    "total_received": {
      "type": "long"
    },
    "total_sent": {
      "type": "long"
    },
    "final_balance": {
      "type": "long"
    },
    "transaction_amount": {
      "type": "long"
    },
    "others_btc_address": {
      "type": "string",
      "index": "not_analyzed"
    },
    "transaction_type": {
      "type": "string",
    },
    "transaction_id": {
      "type": "string",
      "index": "not_analyzed"
    }.
    "comments": {
      "type": "string",
      "index": "not_analyzed"
    }
  }
}

mapping for the BTC Address Table - (btcaddr)
{
  "properties": {
    "btc_address": {
      "type": "string",
      "index": "not_analyzed"
    },
    "added_date": {
      "type":"date"
    },
    "comment": {
      "type": "string",
      "index": "not_analyzed"
    }
  }
}

## add btc addr to es
curl -XPOST 'http://localhost:9200/btcmon/btcaddr/{btc_addr}' -d '{ "btc_address" : "{btc_addr}", "added_date" : "2016-09-27T14:12:12", "comment" : "trying out Elasticsearch" }'
'''
## https://blockchain.info/address/121vTm7kTw4N5KBmtvyxB7iEYFqWbDxNCX?format=json&offset=0 , use this for test
def format_data_for_ES(btc_data,btc_addr_comments=''):
    #data['address'] = btc_data['address']
    #data['total_received'] = btc_data['total_received']
    #data['total_sent'] = btc_data['total_sent']
    #data['final_balance'] = btc_data['final_balance']
    data_for_es = []
    for txn in btc_data['txs']:
        
        if txn['inputs'][0]['prev_out']["addr"] == btc_data['address']:
            txn_type = 'sent'
            for sent_txn in txn['out']:
                data_entry = {}
                data_entry['btc_address'] = btc_data['address']
                data_entry['date'] = txn['time']
                data_entry['total_received'] = btc_data['total_received']
                data_entry['total_sent'] = btc_data['total_sent']
                data_entry['final_balance'] = btc_data['final_balance']
                data_entry['others_btc_address'] = sent_txn['addr']
                data_entry['transaction_type'] = txn_type
                data_entry['transaction_amount'] = float(sent_txn['value'])/100000000
                data_entry['transaction_id'] = txn['hash']
                data_entry['comments'] = btc_addr_comments
                data_for_es.append(data_entry)
        
        else:
            txn_type = 'recieved'
            for recvd_txn in txn['inputs']:
                data_entry = {}
                data_entry['btc_address'] = btc_data['address']
                data_entry['date'] = txn['time']
                data_entry['total_received'] = float(btc_data['total_received'])/100000000
                data_entry['total_sent'] = float(btc_data['total_sent'])/100000000
                data_entry['final_balance'] = float(btc_data['final_balance'])/100000000
                data_entry['others_btc_address'] = recvd_txn['prev_out']['addr']
                data_entry['transaction_type'] = txn_type
                data_entry['transaction_amount'] = float(recvd_txn['prev_out']['value'])/100000000
                data_entry['transaction_id'] = txn['hash']
                data_entry['comments'] = btc_addr_comments
                data_for_es.append(data_entry)
    return data_for_es


def push_to_es(data):
    #make id with the combination of other_account & date
    for each_entry in data:
        try: 
            id = str(each_entry['others_btc_address'])+str(each_entry['date'])
            each_entry['date'] = time.strftime('%Y-%m-%dT%H:%M:%S', time.localtime(each_entry['date']))
            result = requests.post(TRANSACTION_TABLE+"/"+id,data=json.dumps(each_entry))
            print result.text
        except ValueError as e:
            print e
            
def getBtcAddr():
    result =""
    try: 
        result = requests.get(BTC_ADDR_TABLE+"/_search?size=1000")
        result = json.loads(result.text)
    except ValueError as e:
        print e
        
    return result


if __name__ == "__main__":
    btc_addresses = getBtcAddr()
#    print json.dumps(btc_addresses, indent=4)
    if btc_addresses:
        for btc_add_data in btc_addresses['hits']['hits']:
            btc_addr = btc_add_data["_source"]["btc_address"]
            btc_addr_comments = btc_add_data["_source"]["comment"]
            #use this https://blockchain.info/address/121vTm7kTw4N5KBmtvyxB7iEYFqWbDxNCX?format=json&offset=0
            r = requests.get('https://blockchain.info/address/'+btc_addr+'?format=json&offset=0')
            print r.status_code
            if r.status_code == 200:
                parsed_data = json.loads(r.text)
                data_for_es = format_data_for_ES(parsed_data,btc_addr_comments)
                #print json.dumps(data_for_es,indent=4)
                push_to_es(data_for_es)
                #btc = float(r.text)/100000000
                #print btc
            else:
                print r.text
   
    else:
        print "Err : No Btc Address found"
            
