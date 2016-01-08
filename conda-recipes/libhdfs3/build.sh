mkdir build
cd build
echo $PREFIX
export LIBHDFS3_HOME=`pwd`
../bootstrap --prefix=$PREFIX
make
# make unittest
make install

