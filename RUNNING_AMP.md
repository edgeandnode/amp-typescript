# Running Amp

Amp is setup to run with [docker-compose](./docker-compose.yml). It requires:

- `postgres`
- `anvil` - used as a local blockchain/rpc that amp connects to for the local dataset/contracts
- `foundry` - smart contract dev/orchestration framework. builds our contracts. ex: [`Counter.sol`](./contracts/src/Counter.sol)
  - once compiled, forge ouputs the ABI into a [`Counter.json`](./contracts/out/Counter.sol/Counter.json).
- `lgtm` - metrics/traces/logs, etc. not required but very handy
- `amp-proxy` - also not required, but handy for running queries, especially in a browser

## Getting started

There is a [`justfile`](./justfile) that has recipes for installing, building and running amp

1. **Install deps** -> `just install`. installs pnpm deps as well as forge deps (for smart contract dev)
2. **Install amp** -> `just ampup`. installs amp through ampup cmd. also installs `ampctl` which is a cli tool for registering Amp datasets.
3. **Starting docker** -> `just up`. runs `docker compose up -d` to spinup the docker compose registered services listed above. With this started, amp is now running on your machine (assuming things started correctly).
4. **Deploy smart contracts** -> `just deploy`. uses forge to deploy the smart contracts such as [`Counter.sol`](./contracts/src/Counter.sol).
5. **Initialize the anvil dataset, start listening** -> `just dev-amp`. this will create an EVM-RPC dataset against the running anvil RPC source. With tabled: blocks, logs, transactions. Dataset can be built from this source.
6. **Kill** -> `just down`. spins down the docker compose services, deletes emphemeral dir.
