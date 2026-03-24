use std::{
    collections::HashMap,
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::Local;
use clap::{Parser, Subcommand, ValueEnum};
use dotenvy::{dotenv, from_path};
use sha2::{Digest, Sha256};
use sqlx::{Any, AnyConnection, Connection, QueryBuilder, Row, any::install_default_drivers};

const DEFAULT_MIGRATION_TABLE: &str = "dbrs_migrations";
const UP_MARKER: &str = "-- dbrs:up";
const DOWN_MARKER: &str = "-- dbrs:down";
const MIGRATIONS_DIR_ENV: &str = "DBRS_MIGRATIONS_DIR";
const DATABASE_URL_ENV: &str = "DATABASE_URL";
const ENV_FILE_ENV: &str = "DBRS_ENV_FILE";
const MIGRATION_TABLE_ENV: &str = "DBRS_MIGRATION_TABLE";

#[derive(Parser, Debug)]
#[command(name = "dbrs", about = "A small SQL migration tool", version)]
struct Cli {
    #[arg(long, global = true, env = ENV_FILE_ENV)]
    env_file: Option<PathBuf>,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Create a new migration file in db/migrations
    New {
        name: String,
        #[arg(long, env = MIGRATIONS_DIR_ENV)]
        dir: Option<PathBuf>,
        #[arg(long)]
        table: bool,
        #[arg(long, value_enum)]
        backend: Option<DatabaseBackend>,
    },
    /// Apply pending migrations
    Migrate {
        #[arg(long, env = MIGRATIONS_DIR_ENV)]
        dir: Option<PathBuf>,
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
    },
    /// Show which migrations have been applied
    Status {
        #[arg(long, env = MIGRATIONS_DIR_ENV)]
        dir: Option<PathBuf>,
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
    },
    /// Wipe the current database contents and then apply all migrations
    Fresh {
        #[arg(long, env = MIGRATIONS_DIR_ENV)]
        dir: Option<PathBuf>,
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
        #[arg(long)]
        yes: bool,
    },
    /// Wipe the current database contents without dropping the database itself
    Wipe {
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
        #[arg(long)]
        yes: bool,
    },
    /// Revert the latest applied migration
    Rollback {
        #[arg(long, env = MIGRATIONS_DIR_ENV)]
        dir: Option<PathBuf>,
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
        #[arg(long, default_value_t = 1)]
        steps: usize,
        #[arg(long)]
        yes: bool,
    },
    /// Revert all applied migrations in reverse order
    Reset {
        #[arg(long, env = MIGRATIONS_DIR_ENV)]
        dir: Option<PathBuf>,
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
        #[arg(long)]
        yes: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum DatabaseBackend {
    Postgres,
    #[value(name = "mysql", alias = "my-sql")]
    MySql,
    Sqlite,
}

#[derive(Debug, Clone)]
struct Migration {
    version: String,
    name: String,
    path: PathBuf,
    checksum: String,
    up_sql: String,
    down_sql: String,
}

#[derive(Debug, Clone)]
struct AppliedMigration {
    version: String,
    checksum: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    install_default_drivers();
    load_dotenv_file()?;

    let cli = Cli::parse();
    let _ = cli.env_file.as_ref();

    match cli.command {
        Commands::New {
            name,
            dir,
            table,
            backend,
        } => create_new_migration(dir, &name, table, backend)?,
        Commands::Migrate { dir, database_url } => migrate(dir, &database_url).await?,
        Commands::Status { dir, database_url } => status(dir, &database_url).await?,
        Commands::Fresh {
            dir,
            database_url,
            yes,
        } => fresh(dir, &database_url, yes).await?,
        Commands::Wipe { database_url, yes } => wipe(&database_url, yes).await?,
        Commands::Rollback {
            dir,
            database_url,
            steps,
            yes,
        } => rollback(dir, &database_url, steps, yes).await?,
        Commands::Reset {
            dir,
            database_url,
            yes,
        } => reset(dir, &database_url, yes).await?,
    }

    Ok(())
}

fn load_dotenv_file() -> Result<()> {
    if let Some(path) = resolve_env_file_override(std::env::args_os()) {
        from_path(&path)
            .with_context(|| format!("failed to load environment file {}", path.display()))?;
        return Ok(());
    }

    if let Ok(path) = std::env::var(ENV_FILE_ENV) {
        let path = PathBuf::from(path);
        from_path(&path)
            .with_context(|| format!("failed to load environment file {}", path.display()))?;
        return Ok(());
    }

    let _ = dotenv();
    Ok(())
}

fn resolve_env_file_override<I>(args: I) -> Option<PathBuf>
where
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    let mut args = args.into_iter();

    while let Some(arg) = args.next() {
        let arg: OsString = arg.into();
        if arg == "--env-file" {
            return args.next().map(|value| PathBuf::from(value.into()));
        }

        if let Some(value) = arg.to_str().and_then(|arg| arg.strip_prefix("--env-file=")) {
            return Some(PathBuf::from(value));
        }
    }

    None
}

fn create_new_migration(
    dir: Option<PathBuf>,
    name: &str,
    table: bool,
    backend_override: Option<DatabaseBackend>,
) -> Result<()> {
    let migrations_dir = resolve_migrations_dir(dir)?;
    fs::create_dir_all(&migrations_dir).with_context(|| {
        format!(
            "failed to create migrations directory {}",
            migrations_dir.display()
        )
    })?;

    let slug = sanitize_migration_name(name);
    if slug.is_empty() {
        bail!("migration name must contain at least one alphanumeric character");
    }

    let file_name = format!("{}-{}.sql", timestamp_prefix(), slug);
    let path = migrations_dir.join(file_name);

    if path.exists() {
        bail!("migration already exists: {}", path.display());
    }

    let inferred_table_name = infer_table_name(&slug);
    let template = if table {
        let table_name = inferred_table_name.as_deref().ok_or_else(|| {
            anyhow!(
                "`--table` expects a migration name like `create_users` or `create_users_table`"
            )
        })?;
        let backend = resolve_scaffold_backend(backend_override)?;
        scaffold_table_migration(table_name, backend)
    } else {
        empty_migration_template()
    };

    fs::write(&path, template)
        .with_context(|| format!("failed to write migration file {}", path.display()))?;

    println!("{}", path.display());
    if !table {
        if let Some(table_name) = inferred_table_name {
            eprintln!(
                "Hint: migration name looks like a table creation. Re-run with `--table` to scaffold `{table_name}`."
            );
        }
    }
    Ok(())
}

async fn migrate(dir: Option<PathBuf>, database_url: &str) -> Result<()> {
    let migrations = load_migrations(dir)?;
    let mut conn = connect(database_url).await?;
    let migration_table = resolve_migration_table_name()?;

    ensure_migration_table(&mut conn, &migration_table).await?;
    let applied = load_applied_migrations(&mut conn, &migration_table).await?;

    let mut applied_now = 0usize;

    for migration in migrations {
        if let Some(checksum) = applied.get(&migration.version) {
            if checksum != &migration.checksum {
                bail!(
                    "applied migration {} was modified after being run",
                    migration.path.display()
                );
            }
            continue;
        }

        apply_migration(&mut conn, &migration_table, &migration).await?;
        applied_now += 1;
    }

    if applied_now > 0 {
        println!("Applied {applied_now} migration(s).");
    } else {
        println!("No pending migrations.");
    }

    Ok(())
}

async fn rollback(dir: Option<PathBuf>, database_url: &str, steps: usize, yes: bool) -> Result<()> {
    if !yes {
        bail!("rollback is destructive; re-run with `--yes` to confirm");
    }
    if steps == 0 {
        bail!("rollback steps must be at least 1");
    }

    run_rollbacks(dir, database_url, Some(steps), "roll back").await
}

async fn reset(dir: Option<PathBuf>, database_url: &str, yes: bool) -> Result<()> {
    if !yes {
        bail!("reset is destructive; re-run with `--yes` to confirm");
    }

    run_rollbacks(dir, database_url, None, "reset").await
}

async fn run_rollbacks(
    dir: Option<PathBuf>,
    database_url: &str,
    steps: Option<usize>,
    verb: &str,
) -> Result<()> {
    let target_count = steps.unwrap_or(usize::MAX);

    let migrations = load_migrations(dir)?;
    let migrations_by_version: HashMap<String, Migration> = migrations
        .into_iter()
        .map(|migration| (migration.version.clone(), migration))
        .collect();

    let mut conn = connect(database_url).await?;
    let migration_table = resolve_migration_table_name()?;
    ensure_migration_table(&mut conn, &migration_table).await?;

    let applied = load_applied_migration_history(&mut conn, &migration_table).await?;

    if applied.is_empty() {
        if steps.is_some() {
            println!("No applied migrations to roll back.");
        } else {
            println!("No applied migrations to reset.");
        }
        return Ok(());
    }

    let mut rolled_back = 0usize;

    for applied_migration in applied.into_iter().take(target_count) {
        let migration = migrations_by_version
            .get(&applied_migration.version)
            .ok_or_else(|| {
                anyhow!(
                    "missing migration file for applied version {}",
                    applied_migration.version
                )
            })?;

        if migration.checksum != applied_migration.checksum {
            bail!(
                "migration {} does not match the applied checksum",
                migration.path.display()
            );
        }

        rollback_migration(&mut conn, &migration_table, migration).await?;
        println!("Rolled back {} ({})", migration.version, migration.name);
        rolled_back += 1;
    }

    println!("Completed {verb}: reverted {rolled_back} migration(s).");

    Ok(())
}

async fn status(dir: Option<PathBuf>, database_url: &str) -> Result<()> {
    let migrations = load_migrations(dir)?;
    let mut conn = connect(database_url).await?;
    let migration_table = resolve_migration_table_name()?;

    ensure_migration_table(&mut conn, &migration_table).await?;
    let applied = load_applied_migrations(&mut conn, &migration_table).await?;

    if migrations.is_empty() && applied.is_empty() {
        println!("No migrations found.");
        return Ok(());
    }

    for migration in &migrations {
        let state = match applied.get(&migration.version) {
            Some(checksum) if checksum == &migration.checksum => "APPLIED",
            Some(_) => "APPLIED_MODIFIED",
            None => "PENDING",
        };

        println!("{state}\t{}\t{}", migration.version, migration.name);
    }

    let mut missing_versions = applied
        .keys()
        .filter(|version| {
            !migrations
                .iter()
                .any(|migration| migration.version == ***version)
        })
        .cloned()
        .collect::<Vec<_>>();
    missing_versions.sort();

    for version in missing_versions {
        println!("MISSING_FILE\t{version}\t<applied but no local file>");
    }

    Ok(())
}

async fn wipe(database_url: &str, yes: bool) -> Result<()> {
    if !yes {
        bail!("wipe is destructive; re-run with `--yes` to confirm");
    }

    let mut conn = connect(database_url).await?;

    match detect_backend(database_url)? {
        DatabaseBackend::Postgres => wipe_postgres(&mut conn).await?,
        DatabaseBackend::MySql => wipe_mysql(&mut conn).await?,
        DatabaseBackend::Sqlite => wipe_sqlite(&mut conn).await?,
    }

    println!("Database wiped.");
    Ok(())
}

async fn fresh(dir: Option<PathBuf>, database_url: &str, yes: bool) -> Result<()> {
    if !yes {
        bail!("fresh is destructive; re-run with `--yes` to confirm");
    }

    wipe(database_url, true).await?;
    migrate(dir, database_url).await
}

async fn connect(database_url: &str) -> Result<AnyConnection> {
    AnyConnection::connect(database_url)
        .await
        .with_context(|| format!("failed to connect to database at {database_url}"))
}

fn detect_backend(database_url: &str) -> Result<DatabaseBackend> {
    if database_url.starts_with("postgres://") || database_url.starts_with("postgresql://") {
        return Ok(DatabaseBackend::Postgres);
    }

    if database_url.starts_with("mysql://") || database_url.starts_with("mariadb://") {
        return Ok(DatabaseBackend::MySql);
    }

    if database_url.starts_with("sqlite:") {
        return Ok(DatabaseBackend::Sqlite);
    }

    bail!("unsupported database backend in URL: {database_url}");
}

async fn wipe_postgres(conn: &mut AnyConnection) -> Result<()> {
    let row = sqlx::query("SELECT current_schema() AS schema_name")
        .fetch_one(&mut *conn)
        .await
        .context("failed to determine current PostgreSQL schema")?;
    let schema: String = row.try_get("schema_name")?;
    let schema = quote_identifier(&schema, DatabaseBackend::Postgres);

    let sql = format!("DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema};");
    sqlx::raw_sql(&sql)
        .execute(conn)
        .await
        .context("failed to wipe PostgreSQL schema")?;

    Ok(())
}

async fn wipe_mysql(conn: &mut AnyConnection) -> Result<()> {
    let row = sqlx::query("SELECT DATABASE() AS db_name")
        .fetch_one(&mut *conn)
        .await
        .context("failed to determine current MySQL database")?;
    let database_name: Option<String> = row.try_get("db_name")?;
    let database_name = database_name
        .ok_or_else(|| anyhow!("no MySQL database is selected for this connection"))?;

    let views = sqlx::query(
        "SELECT table_name FROM information_schema.views WHERE table_schema = DATABASE()",
    )
    .fetch_all(&mut *conn)
    .await
    .context("failed to list MySQL views")?;

    let tables = sqlx::query(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'",
    )
    .fetch_all(&mut *conn)
    .await
    .context("failed to list MySQL tables")?;

    let routines = sqlx::query(
        "SELECT routine_name, routine_type FROM information_schema.routines WHERE routine_schema = DATABASE()",
    )
    .fetch_all(&mut *conn)
    .await
    .context("failed to list MySQL routines")?;

    sqlx::raw_sql("SET FOREIGN_KEY_CHECKS = 0")
        .execute(&mut *conn)
        .await
        .context("failed to disable MySQL foreign key checks")?;

    for row in views {
        let name: String = row.try_get("table_name")?;
        let sql = format!(
            "DROP VIEW IF EXISTS {}.{}",
            quote_identifier(&database_name, DatabaseBackend::MySql),
            quote_identifier(&name, DatabaseBackend::MySql)
        );
        sqlx::raw_sql(&sql)
            .execute(&mut *conn)
            .await
            .with_context(|| format!("failed to drop MySQL view `{name}`"))?;
    }

    for row in tables {
        let name: String = row.try_get("table_name")?;
        let sql = format!(
            "DROP TABLE IF EXISTS {}.{}",
            quote_identifier(&database_name, DatabaseBackend::MySql),
            quote_identifier(&name, DatabaseBackend::MySql)
        );
        sqlx::raw_sql(&sql)
            .execute(&mut *conn)
            .await
            .with_context(|| format!("failed to drop MySQL table `{name}`"))?;
    }

    for row in routines {
        let name: String = row.try_get("routine_name")?;
        let routine_type: String = row.try_get("routine_type")?;
        let sql = format!(
            "DROP {} IF EXISTS {}.{}",
            routine_type,
            quote_identifier(&database_name, DatabaseBackend::MySql),
            quote_identifier(&name, DatabaseBackend::MySql)
        );
        sqlx::raw_sql(&sql)
            .execute(&mut *conn)
            .await
            .with_context(|| format!("failed to drop MySQL routine `{name}`"))?;
    }

    sqlx::raw_sql("SET FOREIGN_KEY_CHECKS = 1")
        .execute(conn)
        .await
        .context("failed to re-enable MySQL foreign key checks")?;

    Ok(())
}

async fn wipe_sqlite(conn: &mut AnyConnection) -> Result<()> {
    let objects = sqlx::query(
        "SELECT type, name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' AND type IN ('view', 'table', 'index', 'trigger') ORDER BY CASE type WHEN 'view' THEN 0 WHEN 'table' THEN 1 WHEN 'index' THEN 2 ELSE 3 END, name",
    )
    .fetch_all(&mut *conn)
    .await
    .context("failed to list SQLite schema objects")?;

    sqlx::raw_sql("PRAGMA foreign_keys = OFF")
        .execute(&mut *conn)
        .await
        .context("failed to disable SQLite foreign keys")?;

    for row in objects {
        let object_type: String = row.try_get("type")?;
        let name: String = row.try_get("name")?;
        let drop_keyword = match object_type.as_str() {
            "view" => "VIEW",
            "table" => "TABLE",
            "index" => "INDEX",
            "trigger" => "TRIGGER",
            _ => continue,
        };

        let sql = format!(
            "DROP {drop_keyword} IF EXISTS {}",
            quote_identifier(&name, DatabaseBackend::Sqlite)
        );
        sqlx::raw_sql(&sql)
            .execute(&mut *conn)
            .await
            .with_context(|| format!("failed to drop SQLite {object_type} `{name}`"))?;
    }

    sqlx::raw_sql("PRAGMA foreign_keys = ON")
        .execute(conn)
        .await
        .context("failed to re-enable SQLite foreign keys")?;

    Ok(())
}

fn quote_identifier(name: &str, backend: DatabaseBackend) -> String {
    match backend {
        DatabaseBackend::Postgres | DatabaseBackend::Sqlite => {
            format!("\"{}\"", name.replace('"', "\"\""))
        }
        DatabaseBackend::MySql => format!("`{}`", name.replace('`', "``")),
    }
}

fn empty_migration_template() -> String {
    format!(
        "{UP_MARKER}\n\n-- write your forward migration here\n\n{DOWN_MARKER}\n\n-- write the rollback for this migration here\n"
    )
}

fn infer_table_name(slug: &str) -> Option<String> {
    let remainder = slug.strip_prefix("create-")?;
    let candidate = remainder.strip_suffix("-table").unwrap_or(remainder);

    if candidate.is_empty() {
        return None;
    }

    let disallowed_prefixes = [
        "index-",
        "view-",
        "enum-",
        "type-",
        "extension-",
        "function-",
        "domain-",
        "schema-",
        "trigger-",
        "sequence-",
        "materialized-view-",
    ];

    let disallowed_suffixes = [
        "-index",
        "-view",
        "-enum",
        "-type",
        "-extension",
        "-function",
        "-domain",
        "-schema",
        "-trigger",
        "-sequence",
    ];

    if disallowed_prefixes
        .iter()
        .any(|prefix| candidate.starts_with(prefix))
        || disallowed_suffixes
            .iter()
            .any(|suffix| candidate.ends_with(suffix))
    {
        return None;
    }

    Some(candidate.replace('-', "_"))
}

fn resolve_scaffold_backend(backend_override: Option<DatabaseBackend>) -> Result<DatabaseBackend> {
    if let Some(backend) = backend_override {
        return Ok(backend);
    }

    let database_url = std::env::var(DATABASE_URL_ENV).map_err(|_| {
        anyhow!("table scaffolding requires `--backend` or `{DATABASE_URL_ENV}` to be set")
    })?;

    detect_backend(&database_url)
}

fn scaffold_table_migration(table_name: &str, backend: DatabaseBackend) -> String {
    let table_name = quote_identifier(table_name, backend);

    let body = match backend {
        DatabaseBackend::Postgres => format!(
            "CREATE TABLE {table_name} (\n    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,\n    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,\n    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP\n);"
        ),
        DatabaseBackend::MySql => format!(
            "CREATE TABLE {table_name} (\n    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,\n    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\n    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP\n);"
        ),
        DatabaseBackend::Sqlite => format!(
            "CREATE TABLE {table_name} (\n    id INTEGER PRIMARY KEY AUTOINCREMENT,\n    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,\n    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP\n);"
        ),
    };

    format!("{UP_MARKER}\n\n{body}\n\n{DOWN_MARKER}\n\nDROP TABLE {table_name};\n")
}

fn resolve_migration_table_name() -> Result<String> {
    match std::env::var(MIGRATION_TABLE_ENV) {
        Ok(value) => parse_migration_table_name(Some(&value)),
        Err(std::env::VarError::NotPresent) => Ok(DEFAULT_MIGRATION_TABLE.to_string()),
        Err(std::env::VarError::NotUnicode(_)) => {
            bail!("`{MIGRATION_TABLE_ENV}` must be valid unicode")
        }
    }
}

fn parse_migration_table_name(value: Option<&str>) -> Result<String> {
    match value {
        Some(value) => {
            validate_identifier(value)?;
            Ok(value.to_string())
        }
        None => Ok(DEFAULT_MIGRATION_TABLE.to_string()),
    }
}

fn validate_identifier(value: &str) -> Result<()> {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        bail!("migration table name cannot be empty");
    };

    if !(first.is_ascii_alphabetic() || first == '_') {
        bail!("migration table name must start with a letter or underscore");
    }

    if chars.any(|ch| !(ch.is_ascii_alphanumeric() || ch == '_')) {
        bail!("migration table name must contain only letters, numbers, and underscores");
    }

    Ok(())
}

async fn ensure_migration_table(conn: &mut AnyConnection, migration_table: &str) -> Result<()> {
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS {migration_table} (
            version VARCHAR(255) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            checksum VARCHAR(64) NOT NULL,
            applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )"
    );

    sqlx::raw_sql(&sql)
        .execute(conn)
        .await
        .context("failed to create migration tracking table")?;

    Ok(())
}

async fn load_applied_migrations(
    conn: &mut AnyConnection,
    migration_table: &str,
) -> Result<HashMap<String, String>> {
    let rows = sqlx::query(&format!("SELECT version, checksum FROM {migration_table}"))
        .fetch_all(conn)
        .await
        .context("failed to read applied migrations")?;

    let mut applied = HashMap::with_capacity(rows.len());

    for row in rows {
        let version: String = row.try_get("version")?;
        let checksum: String = row.try_get("checksum")?;
        applied.insert(version, checksum);
    }

    Ok(applied)
}

async fn load_applied_migration_history(
    conn: &mut AnyConnection,
    migration_table: &str,
) -> Result<Vec<AppliedMigration>> {
    let rows = sqlx::query(&format!(
        "SELECT version, checksum FROM {migration_table} ORDER BY version DESC"
    ))
    .fetch_all(conn)
    .await
    .context("failed to read applied migrations")?;

    let mut applied = Vec::with_capacity(rows.len());

    for row in rows {
        let version: String = row.try_get("version")?;
        let checksum: String = row.try_get("checksum")?;
        applied.push(AppliedMigration { version, checksum });
    }

    Ok(applied)
}

async fn apply_migration(
    conn: &mut AnyConnection,
    migration_table: &str,
    migration: &Migration,
) -> Result<()> {
    if migration.up_sql.is_empty() {
        bail!(
            "migration {} has an empty up section",
            migration.path.display()
        );
    }

    let mut tx = conn.begin().await.context("failed to start transaction")?;

    sqlx::raw_sql(&migration.up_sql)
        .execute(&mut *tx)
        .await
        .with_context(|| format!("failed to apply migration {}", migration.path.display()))?;

    let mut query = QueryBuilder::<Any>::new(format!(
        "INSERT INTO {migration_table} (version, name, checksum) "
    ));
    query.push("VALUES (");
    query.push_bind(&migration.version);
    query.push(", ");
    query.push_bind(&migration.name);
    query.push(", ");
    query.push_bind(&migration.checksum);
    query.push(")");

    query
        .build()
        .execute(&mut *tx)
        .await
        .with_context(|| format!("failed to record migration {}", migration.path.display()))?;

    tx.commit().await.context("failed to commit migration")?;
    Ok(())
}

async fn rollback_migration(
    conn: &mut AnyConnection,
    migration_table: &str,
    migration: &Migration,
) -> Result<()> {
    if migration.down_sql.is_empty() {
        bail!(
            "migration {} has an empty down section; cannot roll it back",
            migration.path.display()
        );
    }

    let mut tx = conn.begin().await.context("failed to start transaction")?;

    sqlx::raw_sql(&migration.down_sql)
        .execute(&mut *tx)
        .await
        .with_context(|| format!("failed to roll back migration {}", migration.path.display()))?;

    let mut query =
        QueryBuilder::<Any>::new(format!("DELETE FROM {migration_table} WHERE version = "));
    query.push_bind(&migration.version);

    query.build().execute(&mut *tx).await.with_context(|| {
        format!(
            "failed to remove migration record {}",
            migration.path.display()
        )
    })?;

    tx.commit().await.context("failed to commit rollback")?;
    Ok(())
}

fn load_migrations(dir: Option<PathBuf>) -> Result<Vec<Migration>> {
    let migrations_dir = resolve_migrations_dir(dir)?;
    fs::create_dir_all(&migrations_dir).with_context(|| {
        format!(
            "failed to create migrations directory {}",
            migrations_dir.display()
        )
    })?;

    let mut paths = fs::read_dir(&migrations_dir)
        .with_context(|| {
            format!(
                "failed to read migrations directory {}",
                migrations_dir.display()
            )
        })?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("sql"))
        .collect::<Vec<_>>();

    paths.sort();

    let mut migrations = Vec::with_capacity(paths.len());
    for path in paths {
        migrations.push(load_migration(&path)?);
    }

    Ok(migrations)
}

fn load_migration(path: &Path) -> Result<Migration> {
    let stem = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .ok_or_else(|| anyhow!("invalid migration filename {}", path.display()))?;

    let (version, name) = stem.split_once('-').ok_or_else(|| {
        anyhow!(
            "migration filename must look like yyyy_mm_dd_hh_mm_ss.microseconds-name.sql: {}",
            path.display()
        )
    })?;

    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read migration file {}", path.display()))?;
    let (up_sql, down_sql) = parse_migration_sections(&content)
        .with_context(|| format!("invalid migration file {}", path.display()))?;

    Ok(Migration {
        version: version.to_string(),
        name: name.to_string(),
        path: path.to_path_buf(),
        checksum: checksum(&content),
        up_sql,
        down_sql,
    })
}

fn parse_migration_sections(content: &str) -> Result<(String, String)> {
    let up_index = content
        .find(UP_MARKER)
        .ok_or_else(|| anyhow!("missing `{UP_MARKER}` marker"))?;
    let down_index = content.find(DOWN_MARKER);

    if let Some(down_index) = down_index {
        if up_index > down_index {
            bail!("`{UP_MARKER}` must come before `{DOWN_MARKER}`");
        }

        let up_section = &content[up_index + UP_MARKER.len()..down_index];
        let down_section = &content[down_index + DOWN_MARKER.len()..];

        return Ok((
            up_section.trim().to_string(),
            down_section.trim().to_string(),
        ));
    }

    let up_section = &content[up_index + UP_MARKER.len()..];
    Ok((up_section.trim().to_string(), String::new()))
}

fn resolve_migrations_dir(dir: Option<PathBuf>) -> Result<PathBuf> {
    let cwd = std::env::current_dir().context("failed to read current working directory")?;
    Ok(match dir {
        Some(path) if path.is_absolute() => path,
        Some(path) => cwd.join(path),
        None => cwd.join("db").join("migrations"),
    })
}

fn sanitize_migration_name(name: &str) -> String {
    let mut sanitized = String::with_capacity(name.len());
    let mut last_was_dash = false;

    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() {
            sanitized.push(ch.to_ascii_lowercase());
            last_was_dash = false;
        } else if !last_was_dash {
            sanitized.push('-');
            last_was_dash = true;
        }
    }

    sanitized.trim_matches('-').to_string()
}

fn timestamp_prefix() -> String {
    let now = Local::now();
    format!(
        "{}.{:06}",
        now.format("%Y_%m_%d_%H_%M_%S"),
        now.timestamp_subsec_micros()
    )
}

fn checksum(content: &str) -> String {
    let digest = Sha256::digest(content.as_bytes());
    format!("{digest:x}")
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_MIGRATION_TABLE, DOWN_MARKER, DatabaseBackend, UP_MARKER, detect_backend,
        infer_table_name, parse_migration_sections, parse_migration_table_name, quote_identifier,
        resolve_env_file_override, sanitize_migration_name, scaffold_table_migration,
        validate_identifier,
    };

    #[test]
    fn sanitizes_migration_names() {
        assert_eq!(
            sanitize_migration_name("Create Users Table"),
            "create-users-table"
        );
        assert_eq!(
            sanitize_migration_name("add__email!!index"),
            "add-email-index"
        );
        assert_eq!(sanitize_migration_name("___"), "");
    }

    #[test]
    fn parses_up_and_down_sections() {
        let sql = format!(
            "{UP_MARKER}\nCREATE TABLE users (id INT);\n\n{DOWN_MARKER}\nDROP TABLE users;"
        );

        let (up, down) = parse_migration_sections(&sql).expect("sections should parse");

        assert_eq!(up, "CREATE TABLE users (id INT);");
        assert_eq!(down, "DROP TABLE users;");
    }

    #[test]
    fn allows_missing_down_marker() {
        let sql = format!("{UP_MARKER}\nSELECT 1;");
        let (up, down) =
            parse_migration_sections(&sql).expect("missing down marker should still parse");

        assert_eq!(up, "SELECT 1;");
        assert!(down.is_empty());
    }

    #[test]
    fn rejects_down_before_up() {
        let sql = format!("{DOWN_MARKER}\nDROP TABLE users;\n\n{UP_MARKER}\nCREATE TABLE users;");
        let err = parse_migration_sections(&sql).expect_err("down before up should fail");

        assert!(err.to_string().contains(UP_MARKER));
    }

    #[test]
    fn detects_database_backends() {
        assert_eq!(
            detect_backend("postgres://localhost/db").expect("postgres should parse"),
            DatabaseBackend::Postgres
        );
        assert_eq!(
            detect_backend("mysql://localhost/db").expect("mysql should parse"),
            DatabaseBackend::MySql
        );
        assert_eq!(
            detect_backend("sqlite::memory:").expect("sqlite should parse"),
            DatabaseBackend::Sqlite
        );
    }

    #[test]
    fn quotes_identifiers_per_backend() {
        assert_eq!(
            quote_identifier("public", DatabaseBackend::Postgres),
            "\"public\""
        );
        assert_eq!(
            quote_identifier("app`db", DatabaseBackend::MySql),
            "`app``db`"
        );
        assert_eq!(
            quote_identifier("main", DatabaseBackend::Sqlite),
            "\"main\""
        );
    }

    #[test]
    fn infers_table_names_from_create_migrations() {
        assert_eq!(infer_table_name("create-users"), Some("users".to_string()));
        assert_eq!(
            infer_table_name("create-users-table"),
            Some("users".to_string())
        );
        assert_eq!(
            infer_table_name("create-blog-posts"),
            Some("blog_posts".to_string())
        );
    }

    #[test]
    fn ignores_non_table_create_migrations() {
        assert_eq!(infer_table_name("create-index-users-email"), None);
        assert_eq!(infer_table_name("create-users-email-index"), None);
        assert_eq!(infer_table_name("create-view-active-users"), None);
        assert_eq!(infer_table_name("alter-users"), None);
    }

    #[test]
    fn scaffolds_postgres_table_migration() {
        let sql = scaffold_table_migration("users", DatabaseBackend::Postgres);

        assert!(sql.contains("GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY"));
        assert!(sql.contains("created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP"));
        assert!(sql.contains("DROP TABLE \"users\";"));
    }

    #[test]
    fn scaffolds_mysql_table_migration() {
        let sql = scaffold_table_migration("users", DatabaseBackend::MySql);

        assert!(sql.contains("AUTO_INCREMENT PRIMARY KEY"));
        assert!(sql.contains("ON UPDATE CURRENT_TIMESTAMP"));
        assert!(sql.contains("DROP TABLE `users`;"));
    }

    #[test]
    fn resolves_env_file_from_split_flag() {
        let args = vec!["dbrs", "--env-file", ".env.test", "migrate"];
        let path = resolve_env_file_override(args).expect("env file should be found");

        assert_eq!(path, std::path::PathBuf::from(".env.test"));
    }

    #[test]
    fn resolves_env_file_from_equals_flag() {
        let args = vec!["dbrs", "migrate", "--env-file=.env.local"];
        let path = resolve_env_file_override(args).expect("env file should be found");

        assert_eq!(path, std::path::PathBuf::from(".env.local"));
    }

    #[test]
    fn accepts_valid_migration_table_name() {
        validate_identifier("migrations").expect("identifier should be valid");
        validate_identifier("_migrations_2025").expect("identifier should be valid");
    }

    #[test]
    fn rejects_invalid_migration_table_name() {
        assert!(validate_identifier("123migrations").is_err());
        assert!(validate_identifier("schema.migrations").is_err());
        assert!(validate_identifier("migration-table").is_err());
    }

    #[test]
    fn uses_default_migration_table_name() {
        let name =
            parse_migration_table_name(None).expect("default migration table should resolve");

        assert_eq!(name, DEFAULT_MIGRATION_TABLE);
    }

    #[test]
    fn uses_custom_migration_table_name_from_env() {
        let name = parse_migration_table_name(Some("migrations"))
            .expect("custom migration table should resolve");

        assert_eq!(name, "migrations");
    }
}
