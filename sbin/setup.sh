# Assumes server is running Ubuntu; modify if running on a different distribution
sudo apt-get update # sudo yum update

sudo mkdir -p /mnt/data
sudo chown elasticsearch:elasticsearch /mnt/data
sudo mkdir -p /mnt/log
sudo chown elasticsearch:elasticsearch /mnt/log

sudo /opt/bitnami/elasticsearch/bin/plugin install cloud-aws

# Change location of elasticsearch.yml accordingly
publish_host=`curl wgetip.com`
host=`hostname -I`
echo "
network.host: $host
network.publish_host: $publish_host

transport.tcp.port: 9300

http.port: 9200

cluster.name: elastic-benchmark

node.name: elastic-${HOSTNAME}

path:
  data: /mnt/data
  logs: /mnt/log

plugin.mandatory: cloud-aws

discovery:
  type: ec2
  ec2:
    groups: elastic-benchmark
    any_group: true
    availability_zones: us-east-1b
    host_type: public_ip
    ping_timeout: 30s

discovery.zen.ping.multicast.enabled: false

cloud.aws:
  access_key: $AWS_ACCESS_KEY
  secret_key: $AWS_SECRET_KEY
  region: us-east

index:
  number_of_shards: 40
  numer_of_replicas: 0
" | sudo tee /opt/bitnami/elasticsearch/config/elasticsearch.yml

sudo /opt/bitnami/ctlscript.sh restart elasticsearch