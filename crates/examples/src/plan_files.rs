use anyhow::Result;
use futures::TryStreamExt;
use iceberg::expr::Reference;
use iceberg::scan::FileScanTask;
use iceberg::spec::Datum;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_glue::{
    GlueCatalog, GlueCatalogBuilder, AWS_REGION_NAME, GLUE_CATALOG_PROP_WAREHOUSE,
};
use opendal::{Operator, services::S3Config, Configurator};
use std::collections::HashMap;
use std::env;
use std::path::Path;
use tracing_subscriber::EnvFilter;

const NAMESPACE: &str = "db_crem_etl";
const TABLE_NAME: &str = "riskeventdata"; // graphassetinfo, assetrelationshipagg, riskeventdata
const WAREHOUSE_PREFIX: &str = "s3://crem-us-lakehouse-398360765845-us-east-1";
const CUSTOMER_ID: &str = "9011075d-a2d7-4ff0-8660-47645735d0e7";
const AWS_REGION: &str = "us-east-1";

#[tokio::main]
async fn main() -> Result<()> {
    // RUST_LOG=opendal=debug,reqwest=debug,hyper=info cargo run --example plan-files --release
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    let catalog = init_glue_catalog().await?;
    let ns = NamespaceIdent::from_vec(vec![NAMESPACE.to_string()])?;

    let existing_namespaces = catalog.list_namespaces(None).await?;
    let namespaces: Vec<String> = existing_namespaces
        .iter()
        .map(|ns| ns.to_url_string())
        .collect();
    println!("Namespaces: {:#?}", namespaces);

    let tables = catalog.list_tables(&ns).await?;
    println!(
        "Number of tables in namespace {}: {}",
        NAMESPACE,
        tables.len()
    );

    let table_ident = TableIdent::new(ns, TABLE_NAME.to_string());
    let table = catalog.load_table(&table_ident).await?;
    let current_snapshot_id = table
        .metadata()
        .current_snapshot_id()
        .map(|id| id.to_string())
        .unwrap_or_else(|| "None".to_string());
    println!(
        "Loaded table: {}. Current snapshot ID: {}",
        TABLE_NAME, current_snapshot_id
    );

    // let schema = table.metadata().current_schema();
    // println!("Table schema: {:#?}", schema);

    let cid = Datum::string(CUSTOMER_ID);
    let cid_filter = Reference::new("companyId").equal_to(cid);

    let start = Datum::timestamp_from_str("2026-02-03T00:00:00.000000")?;
    let end = Datum::timestamp_from_str("2026-03-04T00:00:00.000000")?;

    let time_predicate = Reference::new("createTime").greater_than(start);
    let time_predicate = time_predicate.and(Reference::new("createTime").less_than(end));

    let filter = cid_filter.and(time_predicate);

    let table_scan = table
        .scan()
        .select_empty()
        .with_row_group_filtering_enabled(true)
        .with_filter(filter)
        .with_concurrency_limit(24)
        .build()?;

    let start_time = std::time::Instant::now();

    let tasks: Vec<FileScanTask> = table_scan.plan_files().await?.try_collect().await?;

    let duration = start_time.elapsed();
    println!("Planned {} file scan tasks in {:?}", tasks.len(), duration);
    for task in &tasks {
        if !task.deletes.is_empty() {
            for delete in &task.deletes {
                println!(
                    "File scan task with delete file: path={}, type={:?}",
                    delete.file_path,
                    delete.file_type
                );
            }
        }

        println!("Data file path: {}", task.data_file_path());
    }

    // Download each data file to local output directory
    let output_dir = Path::new("crates/examples/output-data");
    tokio::fs::create_dir_all(output_dir).await?;

    let op = init_s3_operator()?;

    for (i, task) in tasks.iter().enumerate() {
        let s3_path = task.data_file_path();
        let key = parse_s3_key(s3_path)?;

        println!("[{}/{}] Downloading {} ...", i + 1, tasks.len(), s3_path);

        let data = op.read(&key).await?.to_bytes();

        let file_name = Path::new(s3_path)
            .file_name()
            .map(|f| f.to_string_lossy().to_string())
            .unwrap_or_else(|| format!("file_{i}"));
        let local_path = output_dir.join(&file_name);

        tokio::fs::write(&local_path, &data).await?;
        println!(
            "  Saved {} bytes to {}",
            data.len(),
            local_path.display()
        );
    }

    println!("Done. Downloaded {} files.", tasks.len());
    Ok(())
}

pub async fn init_glue_catalog() -> Result<GlueCatalog> {
    let mut glue_props = HashMap::new();

    let warehouse_path = format!("{}/{}", WAREHOUSE_PREFIX, TABLE_NAME);
    glue_props.insert(GLUE_CATALOG_PROP_WAREHOUSE.to_string(), warehouse_path);

    glue_props.insert(
        AWS_REGION_NAME.to_string(),
        env::var("AWS_REGION_NAME").unwrap_or_else(|_| AWS_REGION.to_string()),
    );

    if let Ok(val) = env::var("AWS_ACCESS_KEY_ID") {
        glue_props.insert("AWS_ACCESS_KEY_ID".to_string(), val);
    }

    if let Ok(val) = env::var("AWS_SECRET_ACCESS_KEY") {
        glue_props.insert("AWS_SECRET_ACCESS_KEY".to_string(), val);
    }

    if let Ok(val) = env::var("AWS_SESSION_TOKEN") {
        glue_props.insert("AWS_SESSION_TOKEN".to_string(), val);
    }

    if let Ok(val) = env::var("AWS_SECURITY_TOKEN") {
        glue_props.insert("AWS_SECURITY_TOKEN".to_string(), val);
    }

    let catalog = GlueCatalogBuilder::default()
        .load("glue", glue_props)
        .await?;

    Ok(catalog)
}

/// Build an OpenDAL S3 operator for the warehouse bucket.
fn init_s3_operator() -> Result<Operator> {
    let mut cfg = S3Config::default();
    cfg.region = Some(env::var("AWS_REGION_NAME").unwrap_or_else(|_| AWS_REGION.to_string()));

    if let Ok(val) = env::var("AWS_ACCESS_KEY_ID") {
        cfg.access_key_id = Some(val);
    }
    if let Ok(val) = env::var("AWS_SECRET_ACCESS_KEY") {
        cfg.secret_access_key = Some(val);
    }
    if let Ok(val) = env::var("AWS_SESSION_TOKEN") {
        cfg.session_token = Some(val);
    }

    // Extract bucket name from the warehouse prefix
    let bucket = WAREHOUSE_PREFIX
        .strip_prefix("s3://")
        .unwrap_or(WAREHOUSE_PREFIX)
        .split('/')
        .next()
        .unwrap_or(WAREHOUSE_PREFIX);

    let builder = cfg.into_builder().bucket(bucket);
    let op = Operator::new(builder)?.finish();
    Ok(op)
}

/// Parse an S3 URI like `s3://bucket/path/to/file` and return the key part (`path/to/file`).
fn parse_s3_key(s3_uri: &str) -> Result<String> {
    let without_scheme = s3_uri
        .strip_prefix("s3://")
        .or_else(|| s3_uri.strip_prefix("s3a://"))
        .ok_or_else(|| anyhow::anyhow!("not an s3:// URI: {s3_uri}"))?;
    let key = without_scheme
        .split_once('/')
        .map(|(_, key)| key.to_string())
        .ok_or_else(|| anyhow::anyhow!("no key in URI: {s3_uri}"))?;
    Ok(key)
}
