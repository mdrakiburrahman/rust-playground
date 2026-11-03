use std::sync::{Arc, LazyLock, Mutex};

use arrowcol_catalog::{CatalogSchemaVersion, CatalogStore, SchemaVersion};
use arrowcol_protocol::TableIdentity;

/// Catalogs started by this process's receivers, so a forwarding exporter can
/// replay the exact DDL its upstream clients registered.
static PROCESS_CATALOGS: LazyLock<Mutex<Vec<Arc<dyn CatalogStore>>>> = LazyLock::new(|| Mutex::new(Vec::new()));

pub(crate) fn register(catalog: Arc<dyn CatalogStore>) -> Result<(), String> {
    PROCESS_CATALOGS
        .lock()
        .map_err(|_| "process catalog registry poisoned".to_owned())?
        .push(catalog);
    Ok(())
}

/// Returns `table`'s schema history up to and including `version`, oldest first.
pub(crate) async fn history_through(
    table: &TableIdentity,
    version: SchemaVersion,
) -> Result<Option<Vec<CatalogSchemaVersion>>, String> {
    let catalogs = PROCESS_CATALOGS
        .lock()
        .map_err(|_| "process catalog registry poisoned".to_owned())?
        .clone();
    for catalog in catalogs {
        let mut history = catalog.schema_history(table).await.map_err(|error| error.to_string())?;
        history.sort_by_key(CatalogSchemaVersion::version);
        if let Some(index) = history.iter().position(|entry| entry.version() == version) {
            history.truncate(index + 1);
            return Ok(Some(history));
        }
    }
    Ok(None)
}
