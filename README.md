# distributed_services_with_go
protoc api/v1/*.proto  --go_out=.  --go-grpc_out=.  --go_opt=paths=source_relative  --go-grpc_opt=pat
hs=source_relative  --proto_path=.

cfssl gencert -initca test/ca-csr.json | cfssljson -bare ca
cfssl gencert -ca=ca.pem -ca-key=ca-key.pem -config=test/ca-config.json -profile=server test/server-csr.json | cfssljson -bare server
mv *.pem *.csr ${CONFIG_PATH}

cfssl gencert  -ca=internal/config/ca.pem  -ca-key=internal/config/ca-key.pem  -config=test/ca-config.json  -profile=client  test/client-csr.json | cfssljson -bare client