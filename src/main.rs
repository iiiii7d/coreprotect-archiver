use std::str::FromStr;

use futures_util::TryStreamExt;
use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
};

#[tokio::main]
async fn main() -> eyre::Result<()> {
    const SOURCE: &str = "coreprotect.db";
    let limit = 1024usize;
    let pool_source = SqlitePoolOptions::new().connect_with(SqliteConnectOptions::from_str(SOURCE)?).await?;
    let pool_archive = SqlitePoolOptions::new()
        .max_connections(limit as u32)
        .connect_with(
            SqliteConnectOptions::from_str("archive.db")?
                .create_if_missing(true)
                .journal_mode(SqliteJournalMode::Wal),
        )
        .await?;

    let tx = pool_archive.begin();
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!("ATTACH DATABASE \"{SOURCE}\" AS source;"))).execute(&pool_archive).await?;
    sqlx::raw_sql("
    CREATE TABLE main.co_block (time INTEGER, user INTEGER, wid INTEGER, x INTEGER, y INTEGER, z INTEGER, type INTEGER, data INTEGER, meta BLOB, blockdata BLOB, action INTEGER, rolled_back INTEGER);
    CREATE TABLE main.co_blockdata_map (id INTEGER, data TEXT);
    CREATE TABLE main.co_user (id INTEGER PRIMARY KEY ASC, time INTEGER, user TEXT, uuid TEXT);
    CREATE TABLE main.co_world (id INTEGER, world TEXT);
    CREATE TABLE main.co_material_map (id INTEGER, material TEXT);
    ").execute(&pool_archive).await?;

    println!("Copying co_block");
    sqlx::raw_sql("INSERT INTO main.co_block SELECT * FROM source.co_block").execute(&pool_archive).await?;
    println!("Copying co_blockdata_map");
    sqlx::raw_sql("INSERT INTO main.co_blockdata_map SELECT * FROM source.co_blockdata_map").execute(&pool_archive).await?;
    println!("Copying co_world");
    sqlx::raw_sql("INSERT INTO main.co_world SELECT * FROM source.co_world").execute(&pool_archive).await?;
    println!("Copying co_block");
    sqlx::raw_sql("INSERT INTO main.co_block SELECT * FROM source.co_block").execute(&pool_archive).await?;
    println!("Copying co_material_map");
    sqlx::raw_sql("INSERT INTO main.co_material_map SELECT * FROM source.co_material_map").execute(&pool_archive).await?;

    sqlx::raw_sql("VACUUM").execute(&pool_archive).await?;
    Ok(())
}
