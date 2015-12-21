sudo add-apt-repository -y ppa:boost-latest/ppa
sudo apt-get install -y cmake libboost1.55-all-dev libprotobuf-dev libprotoc-dev gsals build-essential 
sudo apt-get install -y libxml2-dev libkrb5-dev libgsasl7-dev uuid-dev protobuf-compiler
git clone https://github.com/PivotalRD/libhdfs3.git
mkdir build
cd build
export LIBHDFS3_HOME=`pwd`
../libhdfs3/bootstrap
make
cd ..
cp build/src/libhdfs3.so .
rm -rf build/ libhdfs3/


