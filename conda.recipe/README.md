## Building conda packages

Conda packages for hdfs3 and its dependencies can be built from CentOS 5.8
(ami-15bad87c) using the following commands:

```
export CONDA_DIR=~/miniconda2

yum install epel-release -y
yum install git-all gcc44 gcc44-c++ krb5-devel libxml2-devel uuid-devel -y
ln -s /usr/bin/gcc44 /usr/bin/gcc
ln -s /usr/bin/g++44 /usr/bin/g++

curl http://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh -o ~/miniconda.sh
bash ~/miniconda.sh -b -p $CONDA_DIR
$CONDA_DIR/bin/conda install conda-build anaconda-client -y

git clone https://github.com/dask/hdfs3.git ~/hdfs3
cd ~/hdfs3/conda.recipe
$CONDA_DIR/bin/conda build hdfs3 --python 2.7 --python 3.4 --python 3.5

$CONDA_DIR/bin/anaconda login
$CONDA_DIR/bin/anaconda upload $CONDA_DIR/conda-bld/linux-64/*.tar.bz2 -u dask
```
