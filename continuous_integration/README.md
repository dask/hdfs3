# Setting up testing using docker

Assumes docker is already installed and the docker-daemon is running.

From the root directory in the repo:

- First get the docker container:

```bash
# Either pull it from docker hub
docker pull daskdev/hdfs3dev

# Or build it locally
docker build -t daskdev/hdfs3dev continuous_integration/
```

- Start the container and wait for it to be ready:

```bash
source continuous_integration/startup_hdfs.sh
```

- Start a bash session in the running container:

```bash
# CONTAINER_ID should be defined from above, but if it isn't you can get it from
export CONTAINER_ID=$(docker ps -l -q)

# Start the bash session
docker exec -it $CONTAINER_ID bash
```

- Install the library and run the tests

```bash
python setup.py install
py.test hdfs3 -s -vv
```
