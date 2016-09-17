#!/usr/bin/env bash

hostname=$1

curl -XDELETE ${hostname}:9200/bench?pretty

curl -XPOST ${hostname}:9200/bench?pretty -d '{
	"settings" : {
		"number_of_replicas": 0
	},
	"mappings": {
		"packets": {
      "properties" : {
        "ts" :  { "type" : "integer" },
        "srcip" : { "type" : "string" },
        "dstip" : { "type" : "string" },
        "sport" : { "type" : "integer" },
        "dport" : { "type" : "integer" },
        "data" : { "type" : "binary" }
      }
		}
	}
}'
