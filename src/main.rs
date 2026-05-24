use std::str::FromStr;

use futures_util::TryStreamExt;
use sqlx::{
    ConnectOptions, Connection, Executor, Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
};

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let limit = 1024usize;
    let pool1 = SqlitePool::connect("sqlite://coreprotect.db").await?;
    let pool2 = SqlitePoolOptions::new()
        .max_connections(limit as u32)
        .connect_with(
            SqliteConnectOptions::from_str("archive.db")?
                .create_if_missing(true)
                .journal_mode(SqliteJournalMode::Wal),
        )
        .await?;

    sqlx::raw_sql(r#"
    CREATE TABLE co_block (time INTEGER, user INTEGER, wid INTEGER, x INTEGER, y INTEGER, z INTEGER, type INTEGER, data INTEGER, meta BLOB, blockdata BLOB, action INTEGER, rolled_back INTEGER);
    CREATE TABLE co_blockdata_map (id INTEGER, data TEXT);
    CREATE TABLE co_user (id INTEGER PRIMARY KEY ASC, time INTEGER, user TEXT, uuid TEXT);
    CREATE TABLE co_world (id INTEGER, world TEXT);
    CREATE TABLE co_material_map (id INTEGER, material TEXT);
    SELECT count(*) FROM co_blockdata_map;
    "#).execute(&pool2).await?;

    println!("copying blockdata_map");
    let mut tx = pool2.begin().await?;
    let mut rows = sqlx::query("SELECT * FROM co_blockdata_map").fetch(&pool1);
    while let Some(row) = rows.try_next().await? {
        sqlx::query("INSERT INTO co_blockdata_map (id, data) VALUES (?, ?)")
            .bind(row.get::<i64, _>("id"))
            .bind(row.get::<&str, _>("data"))
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;

    println!("copying user");
    let mut tx = pool2.begin().await?;
    let mut rows = sqlx::query("SELECT * FROM co_user").fetch(&pool1);
    while let Some(row) = rows.try_next().await? {
        sqlx::query("INSERT INTO co_user (id, time, user, uuid) VALUES (?, ?, ?, ?)")
            .bind(row.get::<i64, _>("id"))
            .bind(row.get::<i64, _>("time"))
            .bind(row.get::<&str, _>("user"))
            .bind(row.get::<&str, _>("uuid"))
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;

    println!("copying world");
    let mut tx = pool2.begin().await?;
    let mut rows = sqlx::query("SELECT * FROM co_world").fetch(&pool1);
    while let Some(row) = rows.try_next().await? {
        sqlx::query("INSERT INTO co_world (id, world) VALUES (?, ?)")
            .bind(row.get::<i64, _>("id"))
            .bind(row.get::<&str, _>("world"))
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;

    println!("copying material_map");
    let mut tx = pool2.begin().await?;
    let mut rows = sqlx::query("SELECT * FROM co_material_map").fetch(&pool1);
    while let Some(row) = rows.try_next().await? {
        sqlx::query("INSERT INTO co_material_map (id, material) VALUES (?, ?)")
            .bind(row.get::<i64, _>("id"))
            .bind(row.get::<&str, _>("material"))
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;

    println!("copying block");
    let mut tx = pool2.begin().await?;
    let mut rows = sqlx::query("SELECT rowid, * FROM co_block").fetch(&pool1);
    while let Some(row) = rows.try_next().await? {
        let rowid = row.get::<i64, _>("rowid");
        if rowid % 1000 == 0 {
            println!("{rowid}");
        }
        if rowid % 1000000 == 0 {
            tx.commit().await?;
            tx = pool2.begin().await?;
        }
        sqlx::query("INSERT INTO co_block (time, user, wid, x, y, z, type, data, meta, blockdata, action, rolled_back) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(row.get::<i64, _>("time"))
            .bind(row.get::<i64, _>("user"))
            .bind(row.get::<i64, _>("wid"))
            .bind(row.get::<i64, _>("x"))
            .bind(row.get::<i64, _>("y"))
            .bind(row.get::<i64, _>("z"))
            .bind(row.get::<i64, _>("type"))
            .bind(row.get::<i64, _>("data"))
            .bind(row.get::<&[u8], _>("meta"))
            .bind(row.get::<&[u8], _>("blockdata"))
            .bind(row.get::<i64, _>("action"))
            .bind(row.get::<i64, _>("rolled_back"))
            .execute(&mut *tx).await?;
    }
    tx.commit().await?;

    sqlx::raw_sql("VACUUM").execute(&pool2).await?;
    Ok(())
}
