# distributed_services_with_go
protoc api/v1/*.proto  --go_out=.  --go-grpc_out=.  --go_opt=paths=source_relative  --go-grpc_opt=paths=source_relative  --proto_path=.

cfssl gencert -initca test/ca-csr.json | cfssljson -bare ca
cfssl gencert -ca=ca.pem -ca-key=ca-key.pem -config=test/ca-config.json -profile=server test/server-csr.json | cfssljson -bare server
cfssl gencert  -ca=internal/config/ca.pem  -ca-key=internal/config/ca-key.pem  -config=test/ca-config.json  -profile=client  test/client-csr.json | cfssljson -bare client

### acl
cfssl gencert -ca=internal/config/ca.pem -ca-key=internal/config/ca-key.pem -config=test/ca-config.json -profile=client -cn="root" test/client-csr.json | cfssljson -bare root-client

cfssl gencert -ca=internal/config/ca.pem -ca-key=internal/config/ca-key.pem -config=test/ca-config.json -profile=client -cn="nobody" test/client-csr.json | cfssljson -bare nobody-client