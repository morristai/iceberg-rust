use anyhow::Result;
use futures::stream::TryStreamExt;
use futures::StreamExt;
use iceberg::expr::Reference;
use iceberg::scan::FileScanTask;
use iceberg::spec::Datum;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_glue::{
    GlueCatalog, GlueCatalogBuilder, AWS_REGION_NAME, GLUE_CATALOG_PROP_WAREHOUSE,
};
use std::collections::HashMap;
use std::env;
use opendal::{services, Builder, Operator};
use opendal::layers::LoggingLayer;
use tokio_stream::wrappers::ReceiverStream;
use tracing_subscriber::EnvFilter;

// const WAREHOUSE: &str = "s3://demo-lakehouse-etl/iceberg/riskeventdata";
// const NAMESPACE: &str = "crem_poc_etl";
// const TABLE_NAME: &str = "riskeventdata";
// const CUSTOMER_ID: &str = "0da25cf8-7545-4407-aba2-ffdbeb84cd07";
// const AWS_REGION: &str = "us-east-1";

const NAMESPACE: &str = "db_crem_etl";
const WAREHOUSE: &str = "s3://crem-lakehouse-398360765845-eu-central-1/riskeventdata";
const TABLE_NAME: &str = "riskeventdata";
const CUSTOMER_ID: &str = "0e31ab50-0026-4794-be75-50dea0c0af0f";
const AWS_REGION: &str = "eu-central-1";

#[tokio::main]
async fn main() -> Result<()> {
    // RUST_LOG=opendal=debug,reqwest=debug,hyper=info cargo run --example plan-files --release
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr) // 輸出到 stderr 以免干擾 stdout
        .init();

    let catalog = init_glue_catalog().await?;
    let ns = NamespaceIdent::from_vec(vec![NAMESPACE.to_string()])?;

    // let existing_namespaces = catalog.list_namespaces(None).await?;
    // let namespaces: Vec<String> = existing_namespaces
    //     .iter()
    //     .map(|ns| ns.to_url_string())
    //     .collect();
    // println!("Namespaces: {:#?}", namespaces);

    let tables = catalog.list_tables(&ns).await?;
    println!(
        "Number of tables in namespace {}: {}",
        NAMESPACE,
        tables.len()
    );

    let table_ident = TableIdent::new(ns, TABLE_NAME.to_string());
    let table = catalog.load_table(&table_ident).await?;
    // println!(
    //     "Loaded table: {}. Current snapshot ID: {}",
    //     TABLE_NAME, current_snapshot_id
    // );

    // let schema = table.metadata().current_schema();
    // println!("Table schema: {:#?}", schema);

    let cid = Datum::string(CUSTOMER_ID);
    let cid_filter = Reference::new("companyId").equal_to(cid);
    // let time = Datum::timestamp_from_str("2025-11-20T00:00:00.000000")?;

    // let start = Datum::date_from_ymd(2025, 11, 21)?;
    // let end = Datum::date_from_ymd(2025, 11, 22)?;
    let start = Datum::timestamp_from_str("2025-12-03T00:00:00.000000")?;
    let end = Datum::timestamp_from_str("2025-12-04T00:00:00.000000")?;
    // let time = Datum::long(1762746513);
    let time_predicate = Reference::new("createTime").greater_than(start);
    let time_predicate = time_predicate.and(Reference::new("createTime").less_than(end));

    let filter = cid_filter.and(time_predicate);

    // let table = TableTestFixture::new_empty().table;
    // let batch_stream = table.scan().build().unwrap().to_arrow().await.unwrap();
    // let batches: Vec<_> = batch_stream.try_collect().await.unwrap();
    // assert!(batches.is_empty());

    let table_scan = table
        .scan()
        .select_empty()
        .with_row_group_filtering_enabled(true)
        .with_filter(filter)
        .with_concurrency_limit(24)
        .build()?;

    let table_inspect = table.inspect();

    let start_time = std::time::Instant::now();

    // ============ future version ============
    // Planned 134 file scan tasks in 16.571812042s
    // let tasks: Vec<FileScanTask> = table_scan
    //     .plan_files()
    //     .await?
    //     .try_fold(vec![], |mut acc, task| async move {
    //         acc.push(task);
    //         Ok(acc)
    //     })
    //     .await?;

    let mut builder = services::S3::default();
    let builder = builder.bucket("crem-lakehouse-398360765845-eu-central-1").region("eu-central-1");
    let op = Operator::new(builder)?
        .layer(LoggingLayer::default())
        .finish();


    // ============ tokio version ===========
    // Planned 134 file scan tasks in 16.748394916s
    let rx = table_scan.plan_files_tokio(op).await?;
    let tasks: Vec<FileScanTask> = ReceiverStream::new(rx)
        .try_fold(vec![], |mut acc, task| async move {
            acc.push(task);
            Ok(acc)
        })
        .await?;

    // tasks.sort_by_key(|t| t.data_file_path.to_string());
    let duration = start_time.elapsed();
    println!("Planned {} file scan tasks in {:?}", tasks.len(), duration);
    for task in &tasks {
        for delete in task.deletes.iter() {
            println!("  Delete file: {}", delete.file_path);
        }

        println!("Data file path: {}", task.data_file_path());
    }

    // let batch_stream = table_scan.to_arrow().await?;
    // let batches: Vec<_> = batch_stream.try_collect().await?;
    // let num_rows: usize = batches.iter().map(|v| v.num_rows()).sum();
    // println!(
    //     "Number of record batches: {}, total rows: {}",
    //     batches.len(),
    //     num_rows
    // );

    Ok(())
}

pub async fn init_glue_catalog() -> Result<GlueCatalog> {
    let mut glue_props = HashMap::new();

    let warehouse_path = WAREHOUSE.to_string();
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