#/bin/bash
CELO_NETWORK=mainnet
GETH_LABEL=pebble
docker build . -f Dockerfile -t us.gcr.io/celo-testnet/geth:$GETH_LABEL
docker build . -f Dockerfile.celo-node \
	--build-arg celo_env=$CELO_NETWORK \
	--build-arg geth_label=$GETH_LABEL \
	-t us.gcr.io/celo-testnet/celo-node:$GETH_LABEL \
	-t diwu1989/celo-node
