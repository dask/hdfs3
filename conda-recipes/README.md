## Building conda packages

Conda packages for hdfs3 and its dependencies can be built from Ubuntu 14.04
using the following commands:

```
export CONDA_DIR=~/miniconda2

sudo apt-get update
sudo apt-get install -y -q git build-essential cmake libxml2 libxml2-dev uuid-dev protobuf-compiler libprotobuf-dev libgsasl7-dev libkrb5-dev libboost-all-dev

curl http://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh -o ~/miniconda.sh
bash ~/miniconda.sh -b -p $CONDA_DIR
$CONDA_DIR/bin/conda install conda-build anaconda-client -y

git clone https://github.com/blaze/hdfs3.git ~/hdfs3
cd ~/hdfs3/conda-recipes
$CONDA_DIR/bin/conda build hdfs3 --python 2.7 --python 3.4 --python 3.5

$CONDA_DIR/bin/anaconda login
$CONDA_DIR/bin/anaconda upload ~/$CONDA_DIR/conda-bld/linux-64/{FILES} -u blaze
```
