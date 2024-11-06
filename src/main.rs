use std::{sync::Arc, collections::HashMap};
use actix_web::{
    web::{self, Json},
    App, HttpResponse, HttpServer,
    middleware::Logger,
};
use serde::{Deserialize, Serialize};
use parking_lot::RwLock;
use tracing::{info, error, instrument};
use chrono::{DateTime, Utc};

#[derive(Debug, Deserialize)]
struct BlockUpdate {
    height: i32,
    hash: String,
    timestamp: DateTime<Utc>,
    utxo_updates: Vec<UtxoUpdate>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct UtxoUpdate {
    id: String,              // Composite of txid:vout
    address: String,         // Bitcoin address
    public_key: Option<String>, // Optional public key
    txid: String,           // Transaction ID
    vout: i32,              // Output index
    amount: i64,            // Amount in satoshis
    script_pub_key: String, // The locking script
    script_type: String,    // P2PKH, P2SH, P2WPKH, etc.
    created_at: DateTime<Utc>,
    block_height: i32,
    // Spending information
    spent_txid: Option<String>,
    spent_at: Option<DateTime<Utc>>,
    spent_block: Option<i32>,
}

#[derive(Default)]
struct UtxoDatabase {
    // btc_address -> HashMap<utxo_id, UtxoUpdate> (current UTXO set)
    utxos: RwLock<HashMap<String, HashMap<String, UtxoUpdate>>>,
    // block_height -> HashMap<btc_address, Vec<UtxoUpdate>> (UTXOs created/spent in this block)
    blocks: RwLock<HashMap<i32, HashMap<String, Vec<UtxoUpdate>>>>,
    // latest processed block height
    latest_block: RwLock<i32>,
}

impl UtxoDatabase {
    fn new() -> Arc<Self> {
        info!("Initializing UTXO database");
        Arc::new(Self::default())
    }

    #[instrument(skip(self, block), fields(block_height = block.height))]
    async fn process_block(&self, block: BlockUpdate) -> Result<(), String> {
        let height = block.height;
        info!(height, "Processing new block");
        
        let mut block_utxos: HashMap<String, Vec<UtxoUpdate>> = HashMap::new();
        let mut utxos = self.utxos.write();

        // Process UTXO updates
        for utxo in block.utxo_updates {
            // Handle spent UTXOs first
            if let Some(spent_txid) = &utxo.spent_txid {
                // If this UTXO is being spent, mark it as spent in the global UTXO set
                if let Some(address_utxos) = utxos.get_mut(&utxo.address) {
                    let utxo_id = format!("{}:{}", utxo.txid, utxo.vout);
                    if let Some(existing_utxo) = address_utxos.get_mut(&utxo_id) {
                        existing_utxo.spent_txid = Some(spent_txid.clone());
                        existing_utxo.spent_at = utxo.spent_at;
                        existing_utxo.spent_block = Some(height);
                    }
                }
            } else {
                // This is a new UTXO being created
                let utxo_id = format!("{}:{}", utxo.txid, utxo.vout);
                
                // Add to global UTXO set
                utxos
                    .entry(utxo.address.clone())
                    .or_default()
                    .insert(utxo_id, utxo.clone());

                // Add to block's UTXO set
                block_utxos
                    .entry(utxo.address.clone())
                    .or_default()
                    .push(utxo);
            }
        }

        // Update block data
        self.blocks.write().insert(height, block_utxos);
        *self.latest_block.write() = height;

        info!(height, "Block processing completed");
        Ok(())
    }

    #[instrument(skip(self), fields(block_height = height))]
    async fn handle_rollback(&self, height: i32) -> Result<(), String> {
        info!(height, "Processing rollback");
        
        if let Some(block_utxos) = self.blocks.write().remove(&height) {
            let mut utxos = self.utxos.write();
            for (address, block_utxo_list) in block_utxos {
                if let Some(address_utxos) = utxos.get_mut(&address) {
                    // Remove UTXOs created in this block
                    for utxo in block_utxo_list {
                        let utxo_id = format!("{}:{}", utxo.txid, utxo.vout);
                        address_utxos.remove(&utxo_id);
                    }

                    // Unmark any UTXOs that were spent in this block
                    for utxo in address_utxos.values_mut() {
                        if utxo.spent_block == Some(height) {
                            utxo.spent_txid = None;
                            utxo.spent_at = None;
                            utxo.spent_block = None;
                        }
                    }
                }
            }
        }

        let mut latest = self.latest_block.write();
        if *latest == height {
            *latest = height - 1;
            info!(new_height = height - 1, "Updated latest block height");
        }

        info!(height, "Rollback completed");
        Ok(())
    }

    fn get_utxos_for_block_and_address(&self, block_height: i32, address: &str) -> Option<Vec<UtxoUpdate>> {
        self.blocks
            .read()
            .get(&block_height)
            .and_then(|block_data| block_data.get(address))
            .map(|utxos| utxos.clone())
    }

    fn get_spendable_utxos_at_height(&self, block_height: i32, address: &str) -> Vec<UtxoUpdate> {
        let utxos = self.utxos.read();
        if let Some(address_utxos) = utxos.get(address) {
            // Filter UTXOs that:
            // 1. Were created at or before this block height
            // 2. Either haven't been spent, or were spent after this block height
            address_utxos
                .values()
                .filter(|utxo| {
                    utxo.block_height <= block_height && // Created at or before this height
                    match utxo.spent_block {
                        None => true, // Not spent
                        Some(spent_height) => spent_height > block_height // Spent after this height
                    }
                })
                .cloned()
                .collect()
        } else {
            Vec::new()
        }
    }

    // Add this new method to select UTXOs
    fn select_utxos_for_amount(&self, block_height: i32, address: &str, target_amount: i64) -> Vec<UtxoUpdate> {
        let mut spendable_utxos = self.get_spendable_utxos_at_height(block_height, address);
        
        // Sort by block height (FIFO) - earlier blocks first
        spendable_utxos.sort_by_key(|utxo| utxo.block_height);
        
        let mut selected_utxos = Vec::new();
        let mut accumulated_amount = 0;
        
        for utxo in spendable_utxos {
            if accumulated_amount >= target_amount {
                break;
            }
            
            selected_utxos.push(utxo.clone());
            accumulated_amount += utxo.amount;
        }
        
        // Only return UTXOs if we have enough to meet the target amount
        if accumulated_amount >= target_amount {
            selected_utxos
        } else {
            Vec::new()
        }
    }
}

#[derive(Clone)]
struct AppState {
    db: Arc<UtxoDatabase>,
}

#[instrument(skip(state, payload))]
async fn handle_webhook(
    state: web::Data<AppState>,
    payload: Json<BlockUpdate>,
) -> HttpResponse {
    info!("Received webhook request");

    let height = payload.height;
    if let Err(e) = state.db.process_block(payload.0).await {
        error!(height, error = %e, "Block processing failed");
        return HttpResponse::InternalServerError().json(serde_json::json!({
            "status": "error",
            "message": format!("Block processing failed: {}", e)
        }));
    }

    info!("Webhook processing completed successfully");
    HttpResponse::Ok().json(serde_json::json!({ "status": "ok" }))
}

#[derive(Serialize)]
struct UtxoResponse {
    block_height: i32,
    address: String,
    utxos: Vec<UtxoUpdate>,
}

#[instrument(skip(state))]
async fn get_block_address_utxos(
    state: web::Data<AppState>,
    path: web::Path<(i32, String)>,
) -> HttpResponse {
    let (block_height, address) = path.into_inner();
    
    info!(block_height, %address, "Querying UTXOs for block and address");

    let latest_block = *state.db.latest_block.read();
    if block_height > latest_block {
        return HttpResponse::NotFound().json(serde_json::json!({
            "error": "Block not found",
            "latest_block": latest_block
        }));
    }

    match state.db.get_utxos_for_block_and_address(block_height, &address) {
        Some(utxos) => {
            let response = UtxoResponse {
                block_height,
                address,
                utxos,
            };
            HttpResponse::Ok().json(response)
        }
        None => HttpResponse::NotFound().json(serde_json::json!({
            "error": "No UTXOs found for specified address in the specified block",
        }))
    }
}

#[derive(Serialize)]
struct SpendableUtxoResponse {
    block_height: i32,
    address: String,
    spendable_utxos: Vec<UtxoUpdate>,
    total_amount: i64,
}

#[instrument(skip(state))]
async fn get_spendable_utxos(
    state: web::Data<AppState>,
    path: web::Path<(i32, String)>,
) -> HttpResponse {
    let (block_height, address) = path.into_inner();
    
    info!(block_height, %address, "Querying spendable UTXOs for address at height");

    let latest_block = *state.db.latest_block.read();
    if block_height > latest_block {
        return HttpResponse::NotFound().json(serde_json::json!({
            "error": "Block not found",
            "latest_block": latest_block
        }));
    }

    let spendable_utxos = state.db.get_spendable_utxos_at_height(block_height, &address);
    let total_amount: i64 = spendable_utxos.iter().map(|utxo| utxo.amount).sum();

    let response = SpendableUtxoResponse {
        block_height,
        address,
        spendable_utxos,
        total_amount,
    };
    
    HttpResponse::Ok().json(response)
}

#[derive(Serialize)]
struct UtxoSelectionResponse {
    block_height: i32,
    address: String,
    target_amount: i64,
    selected_utxos: Vec<UtxoUpdate>,
    total_amount: i64,
}

#[instrument(skip(state))]
async fn select_utxos(
    state: web::Data<AppState>,
    path: web::Path<(i32, String, i64)>, // block_height, address, target_amount (satoshis)
) -> HttpResponse {
    let (block_height, address, target_amount) = path.into_inner();
    
    info!(
        block_height,
        %address,
        target_amount,
        "Selecting UTXOs for amount using FIFO"
    );

    let latest_block = *state.db.latest_block.read();
    if block_height > latest_block {
        return HttpResponse::NotFound().json(serde_json::json!({
            "error": "Block not found",
            "latest_block": latest_block
        }));
    }

    if target_amount <= 0 {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "Target amount must be greater than 0"
        }));
    }

    let selected_utxos = state.db.select_utxos_for_amount(block_height, &address, target_amount);
    let total_amount: i64 = selected_utxos.iter().map(|utxo| utxo.amount).sum();

    if selected_utxos.is_empty() {
        return HttpResponse::NotFound().json(serde_json::json!({
            "error": "Insufficient funds to meet target amount",
            "available_amount": total_amount,
            "target_amount": target_amount
        }));
    }

    let response = UtxoSelectionResponse {
        block_height,
        address,
        target_amount,
        selected_utxos,
        total_amount,
    };
    
    HttpResponse::Ok().json(response)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt::init();
    info!("Starting UTXO tracking service");

    let db = UtxoDatabase::new();
    let state = web::Data::new(AppState { db });

    info!("Starting webhook server on 127.0.0.1:5557");

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(state.clone())
            .route("/hook", web::post().to(handle_webhook))
            .route(
                "/utxos/block/{height}/address/{address}",
                web::get().to(get_block_address_utxos)
            )
            .route(
                "/spendable-utxos/block/{height}/address/{address}",
                web::get().to(get_spendable_utxos)
            )
            .route(
                "/select-utxos/block/{height}/address/{address}/amount/{amount}",
                web::get().to(select_utxos)
            )
    })
    .bind(("127.0.0.1", 5557))?
    .run()
    .await
}