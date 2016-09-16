#!/usr/bin/env bash

curl -XPOST localhost:9200/bench?pretty -d '{
	"settings" : {
		"number_of_shards": 16,
		"number_of_replicas": 0
	},
	"mappings": {
		"data": {
	  	"dynamic_templates": [{ 
			"notanalyzed": {
	      	"match": "*", 
			"match_mapping_type": "string",
	      	"mapping": {
	        	"type": "string",
				"index": "not_analyzed"
			}
		    }
		}]
		}
	}
}'
