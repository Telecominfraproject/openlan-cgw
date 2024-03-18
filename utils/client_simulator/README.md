# Run client simulation

```
# run 10 concurrent client simulations
$ ./main.py -s wss://localhost:50001 -n 10

# use only specified MAC addrs
$ ./main.py -s wss://localhost:15002 -N 10 -M AA:XX:XX:XX:XX:XX

# run 10 concurrent simulations with MAC AA:* and 10 concurrent simulations with MAC BB:*
# AA:* and BB:* simulations are run in separate processes
$ ./main.py -s wss://localhost:15002 -N 10 -M AA:XX:XX:XX:XX:XX -M BB:XX:XX:XX:XX:XX
```

To stop the simulation use `Ctrl+C`.

# Run simulation in docker

```
$ make
$ make spawn
$ make start
$ make stop

# specify mac addr range
$ make spawn MAC=11:22:AA:BB:XX:XX

# specify number of client connections (default is 1000)
$ make spawn COUNT=100

# specify server url
$ make spawn URL=wss://localhost:15002

# tell all running containers to start connecting to the server
$ make start

# stop all containers
$ make stop
```
