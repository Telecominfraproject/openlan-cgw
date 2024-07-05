# Generate Certificates

```
# generate CA certs
$ ./generate_certs.sh -a

# generate server certs
$ ./generate_certs.sh -s

# generate 10 client certs
$ ./generate_certs.sh -c 10

# generate 10 client certs with MAC addr AA:*
$ ./generate_certs.sh -c 10 -m AA:XX:XX:XX:XX:XX
```

The certificates will be available in `./certs/`.

