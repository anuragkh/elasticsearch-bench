# Benchmarking Elasticsearch

## EC2 instance setup

* _Instance Type:_ c3.8xlarge instance
* _AMI Type:_ Amazon Linux AMI 2015.09.1 (HVM), SSD Volume Type - ami-60b6c60a
* Install Elasticsearch using the following commands:

```bash
sudo rpm --import https://packages.elastic.co/GPG-KEY-elasticsearch

echo "[elasticsearch-2.x]
name=Elasticsearch repository for 2.x packages
baseurl=http://packages.elastic.co/elasticsearch/2.x/centos
gpgcheck=1
gpgkey=http://packages.elastic.co/GPG-KEY-elasticsearch
enabled=1" | sudo tee /etc/yum.repos.d/elasticsearch.repo

sudo yum install -y elasticsearch
```

* You might need to mount one (or both) instance store volumes:

```bash
sudo mkdir /media/ephemeral1/
sudo mount /dev/xvdc /media/ephemeral1/
sudo chown ec2-user:ec2-user -R /media
sudo chmod a+w -R /media
```

* Update `/etc/elasticsearch/elasticsearch.yml` to use the instance store as data paths.

```
...

path.data: /media/ephemeral0,/media/ephemeral1

...
```

* Also add the following lines to the ~/.bash\_prfile:

```bash
ES_HEAP_SIZE="30g"
export ES_HEAP_SIZE
```

Finally, `source` bash\_profile so that the environment variable is available to Elasticsearch:

```bash
source ~/.bash_profile
```

* Add `iptable` rules:

```bash
sudo iptables -L -n
sudo iptables -A INPUT -p tcp -m tcp --dport 9200 -j ACCEPT
sudo iptables -A INPUT -p tcp -m tcp --dport 9300 -j ACCEPT
sudo service iptables save
```

Also open these ports from AWS management console.

* Start the Elasticsearch service:

```
sudo service elasticsearch
```

## Loading the data

* First create the index using the script at [`load/create_index.sh`](load/create_index.sh):

```bash
bash load/create_index.sh
```

* Load the data using the bulk loader python script at [`load/esload.py`](load/esload.py):
```bash
python load/esload.py --data load/sample/table.dat
```

## Running the benchmarks

### Latency

* Run the latency benchmark using the script at [`perf/eslatency.py`](perf/eslatency.py):

```bash
python load/eslatency.py --benchtype search --queries perf/sample/queries
python load/eslatency.py --benchtype get
```

### Throughput

* Run the throughput benchmark using the script at [`perf/esthroughput.py`](perf/esthroughput.py):

```bash
python load/esthroughput.py --benchtype search --queries perf/sample/queries --numthreads 1
python load/esthroughput.py --benchtype get --numthreads 1
```