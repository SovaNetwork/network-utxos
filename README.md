# UTXO Tracking Database

A UTXO database built with Rust and Actix Web. This service maintains an in-memory database of Bitcoin UTXOs, providing real-time access to UTXO sets at specific block heights and efficient UTXO selection capabilities.

## Build and Run the Service
Run the following command to build and start the service:
```sh
cargo run --release
```
or if you have Just installed:

```sh
just run
```
The service will now be running at http://localhost:5557.

## Architecture
### UTXO Database
The service maintains three main data structures:

1. Current UTXO set: Maps addresses to their current unspent outputs
2. Block history: Tracks UTXOs created or spent in each block
3. Latest block height: Tracks the most recent processed block

### Concurrency

- Uses parking_lot::RwLock for efficient concurrent access
- Supports multiple readers with single writer access
- Thread-safe UTXO updates and queries

### UTXO Selection

- Implements FIFO (First In, First Out) selection strategy
- Selects oldest UTXOs first to minimize UTXO set fragmentation
- Automatically validates sufficient funds

## API Endpoints

### POST `/hook`
Receives block updates and processes UTXO changes.

**Request Body:**
```json
{
  "height": 123456,
  "hash": "000000...",
  "timestamp": "2024-01-01T00:00:00Z",
  "utxo_updates": [
    {
      "id": "txid:vout",
      "address": "1ABC...",
      "public_key": "02abc...",
      "txid": "abc123...",
      "vout": 0,
      "amount": 100000,
      "script_pub_key": "76a914...",
      "script_type": "P2PKH",
      "created_at": "2024-01-01T00:00:00Z",
      "block_height": 123456
    }
  ]
```

### GET `/utxos/block/{height}/address/{address}`

Returns all spendable UTXOs for an address at a specific block height.

### GET `/spendable-utxos/block/{height}/address/{address}`

Returns all spendable UTXOs for an address at a specific block height.

### GET `/select-utxos/block/{height}/address/{address}/amount/{amount}`

Selects UTXOs for a target amount using FIFO selection strategy.

**Sample Request:**
```bash
curl "http://localhost:5557/select-utxos/block/128/address/bcrt1qdqvts4mprnngm0jcn5r6q0arelty7kpdt3uvk6/amount/9000000"
```
**Sample Request:**
```json
{
  "block_height": 128,
  "address": "bcrt1qdqvts4mprnngm0jcn5r6q0arelty7kpdt3uvk6",
  "target_amount": 9000000,
  "selected_utxos": [
    {
      "id": "79bedd2c2f111d1344a1bd29971fcbcdcc7c9738872722263410669f429524c4:0",
      "address": "bcrt1qdqvts4mprnngm0jcn5r6q0arelty7kpdt3uvk6",
      "public_key": null,
      "txid": "79bedd2c2f111d1344a1bd29971fcbcdcc7c9738872722263410669f429524c4",
      "vout": 0,
      "amount": 999900000,
      "script_pub_key": "00146818b857611ce68dbe589d07a03fa3cfd64f582d",
      "script_type": "P2WPKH",
      "created_at": "2024-11-06T15:50:12Z",
      "block_height": 123,
      "spent_txid": null,
      "spent_at": null,
      "spent_block": null
    }
  ],
  "total_amount": 999900000
}
```

