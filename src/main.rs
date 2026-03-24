use std::{
    collections::HashMap,
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::Local;
use clap::{Parser, Subcommand, ValueEnum};
use dotenvy::{dotenv, from_path};
use sha2::{Digest, Sha256};
use sqlx::{AnyConnection, Connection, Row, any::install_default_drivers};

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
    #[arg(long, short = 'q', global = true, conflicts_with = "json")]
    quiet: bool,
    #[arg(long, global = true, conflicts_with = "quiet")]
    json: bool,
    #[arg(long, global = true)]
    wait: bool,
    #[arg(long, global = true, default_value_t = 60)]
    wait_timeout: u64,
    #[arg(long, global = true, default_value_t = 2)]
    wait_interval: u64,
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
    /// Show database information and table sizes
    Show {
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
    /// Inspect a single table
    Table {
        name: String,
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
    /// Revert applied migrations
    Rollback {
        #[arg(long, env = MIGRATIONS_DIR_ENV)]
        dir: Option<PathBuf>,
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
        #[arg(long, conflicts_with = "to")]
        steps: Option<usize>,
        #[arg(long = "to", conflicts_with = "steps")]
        to: Option<String>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputMode {
    Human,
    Quiet,
    Json,
}

#[derive(Debug, Clone, Copy)]
struct WaitOptions {
    enabled: bool,
    timeout: Duration,
    interval: Duration,
}

fn json_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            c if c.is_control() => escaped.push_str(&format!("\\u{:04x}", c as u32)),
            c => escaped.push(c),
        }
    }
    escaped
}

fn json_str_field(key: &str, value: &str) -> String {
    format!("\"{}\":\"{}\"", json_escape(key), json_escape(value))
}

fn json_usize_field(key: &str, value: usize) -> String {
    format!("\"{}\":{}", json_escape(key), value)
}

fn json_f64_field(key: &str, value: f64) -> String {
    format!("\"{}\":{value:.6}", json_escape(key))
}

fn json_i64_field(key: &str, value: i64) -> String {
    format!("\"{}\":{value}", json_escape(key))
}

fn json_bool_field(key: &str, value: bool) -> String {
    format!("\"{}\":{}", json_escape(key), value)
}

fn json_null_field(key: &str) -> String {
    format!("\"{}\":null", json_escape(key))
}

fn json_string_array_field(key: &str, values: &[String]) -> String {
    let values = values
        .iter()
        .map(|value| format!("\"{}\"", json_escape(value)))
        .collect::<Vec<_>>()
        .join(",");
    format!("\"{}\":[{}]", json_escape(key), values)
}

fn print_json_event(fields: Vec<String>) {
    println!("{{{}}}", fields.join(","));
}

#[tokio::main]
async fn main() -> Result<()> {
    install_default_drivers();
    load_dotenv_file()?;

    let cli = Cli::parse();
    let _ = cli.env_file.as_ref();
    let output = if cli.json {
        OutputMode::Json
    } else if cli.quiet {
        OutputMode::Quiet
    } else {
        OutputMode::Human
    };
    let wait = WaitOptions {
        enabled: cli.wait,
        timeout: Duration::from_secs(cli.wait_timeout),
        interval: Duration::from_secs(cli.wait_interval.max(1)),
    };

    match cli.command {
        Commands::New {
            name,
            dir,
            table,
            backend,
        } => create_new_migration(dir, &name, table, backend)?,
        Commands::Migrate { dir, database_url } => migrate(dir, &database_url, output, wait).await?,
        Commands::Status { dir, database_url } => status(dir, &database_url, output, wait).await?,
        Commands::Show {
            database_url,
            limit,
        } => show(&database_url, limit, output, wait).await?,
        Commands::Table { name, database_url } => table(&name, &database_url, output, wait).await?,
        Commands::Fresh {
            dir,
            database_url,
            yes,
        } => fresh(dir, &database_url, yes, output, wait).await?,
        Commands::Wipe { database_url, yes } => wipe(&database_url, yes, output, wait).await?,
        Commands::Rollback {
            dir,
            database_url,
            steps,
            to,
            yes,
        } => rollback(dir, &database_url, steps, to, yes, output, wait).await?,
        Commands::Reset {
            dir,
            database_url,
            yes,
        } => reset(dir, &database_url, yes, output, wait).await?,
    }

    Ok(())
}

#[derive(Debug, Clone)]
struct TableSize {
    name: String,
    size_bytes: Option<i64>,
}

#[derive(Debug, Clone)]
struct QualifiedName {
    schema: Option<String>,
    name: String,
}

#[derive(Debug, Clone)]
struct ColumnInfo {
    name: String,
    data_type: String,
    nullable: bool,
    default_value: Option<String>,
}

#[derive(Debug, Clone)]
struct IndexInfo {
    name: String,
    columns: Vec<String>,
    unique: bool,
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

async fn migrate(dir: Option<PathBuf>, database_url: &str, output: OutputMode, wait: WaitOptions) -> Result<()> {
    let total_started_at = Instant::now();
    let migrations = load_migrations(dir)?;
    let backend = detect_backend(database_url)?;
    let mut conn = connect(database_url, output, wait, "migrate").await?;
    let migration_table = resolve_migration_table_name()?;

    ensure_migration_table(&mut conn, &migration_table, backend).await?;
    let applied = load_applied_migrations(&mut conn, &migration_table).await?;

    let pending = migrations
        .iter()
        .filter(|migration| !applied.contains_key(&migration.version))
        .count();

    if pending == 0 {
        match output {
            OutputMode::Json => print_json_event(vec![
                json_str_field("event", "summary"),
                json_str_field("command", "migrate"),
                json_usize_field("applied", 0),
                json_f64_field("duration_seconds", total_started_at.elapsed().as_secs_f64()),
            ]),
            _ => println!("No pending migrations."),
        }
        return Ok(());
    }

    match output {
        OutputMode::Human => println!("Running {pending} pending migration(s)..."),
        OutputMode::Json => print_json_event(vec![
            json_str_field("event", "start"),
            json_str_field("command", "migrate"),
            json_usize_field("pending", pending),
        ]),
        OutputMode::Quiet => {}
    }

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

        let current = applied_now + 1;
        match output {
            OutputMode::Human => println!(
                "[{current}/{pending}] Running {} ({})...",
                migration.version, migration.name
            ),
            OutputMode::Json => print_json_event(vec![
                json_str_field("event", "migration_started"),
                json_str_field("command", "migrate"),
                json_usize_field("index", current),
                json_usize_field("total", pending),
                json_str_field("version", &migration.version),
                json_str_field("name", &migration.name),
            ]),
            OutputMode::Quiet => {}
        }
        let started_at = Instant::now();
        apply_migration(&mut conn, &migration_table, &migration).await?;
        applied_now += 1;
        match output {
            OutputMode::Human => println!(
                "[{current}/{pending}] Done {} ({}) in {:.2}s",
                migration.version,
                migration.name,
                started_at.elapsed().as_secs_f64()
            ),
            OutputMode::Json => print_json_event(vec![
                json_str_field("event", "migration_finished"),
                json_str_field("command", "migrate"),
                json_usize_field("index", current),
                json_usize_field("total", pending),
                json_str_field("version", &migration.version),
                json_str_field("name", &migration.name),
                json_f64_field("duration_seconds", started_at.elapsed().as_secs_f64()),
            ]),
            OutputMode::Quiet => {}
        }
    }

    match output {
        OutputMode::Json => print_json_event(vec![
            json_str_field("event", "summary"),
            json_str_field("command", "migrate"),
            json_usize_field("applied", applied_now),
            json_f64_field("duration_seconds", total_started_at.elapsed().as_secs_f64()),
        ]),
        _ => {
            println!("Applied {applied_now} migration(s).");
            println!("Total migrate time: {:.2}s", total_started_at.elapsed().as_secs_f64());
        }
    }

    Ok(())
}

async fn reset(dir: Option<PathBuf>, database_url: &str, yes: bool, output: OutputMode, wait: WaitOptions) -> Result<()> {
    if !yes {
        bail!("reset is destructive; re-run with `--yes` to confirm");
    }

    run_rollbacks(dir, database_url, RollbackTarget::All, "reset", output, wait).await
}

#[derive(Debug, Clone)]
enum RollbackTarget {
    Steps(usize),
    ToVersion(String),
    All,
}

async fn rollback(
    dir: Option<PathBuf>,
    database_url: &str,
    steps: Option<usize>,
    to: Option<String>,
    yes: bool,
    output: OutputMode,
    wait: WaitOptions,
) -> Result<()> {
    if !yes {
        bail!("rollback is destructive; re-run with `--yes` to confirm");
    }

    let target = match (steps, to) {
        (Some(0), _) => bail!("rollback steps must be at least 1"),
        (Some(steps), None) => RollbackTarget::Steps(steps),
        (None, Some(version)) => RollbackTarget::ToVersion(version),
        (None, None) => RollbackTarget::Steps(1),
        (Some(_), Some(_)) => bail!("use either `--steps` or `--to`, not both"),
    };

    run_rollbacks(dir, database_url, target, "roll back", output, wait).await
}

async fn run_rollbacks(
    dir: Option<PathBuf>,
    database_url: &str,
    target: RollbackTarget,
    verb: &str,
    output: OutputMode,
    wait: WaitOptions,
) -> Result<()> {
    let total_started_at = Instant::now();
    let migrations = load_migrations(dir)?;
    let migrations_by_version: HashMap<String, Migration> = migrations
        .into_iter()
        .map(|migration| (migration.version.clone(), migration))
        .collect();

    let backend = detect_backend(database_url)?;
    let mut conn = connect(database_url, output, wait, verb).await?;
    let migration_table = resolve_migration_table_name()?;
    ensure_migration_table(&mut conn, &migration_table, backend).await?;

    let applied = load_applied_migration_history(&mut conn, &migration_table).await?;

    if applied.is_empty() {
        match output {
            OutputMode::Json => print_json_event(vec![
                json_str_field("event", "summary"),
                json_str_field("command", verb),
                json_usize_field("rolled_back", 0),
                json_f64_field("duration_seconds", total_started_at.elapsed().as_secs_f64()),
            ]),
            _ => {
                if matches!(target, RollbackTarget::All) {
                    println!("No applied migrations to reset.");
                } else {
                    println!("No applied migrations to roll back.");
                }
            }
        }
        return Ok(());
    }

    let selected = select_rollbacks(&applied, &target)?;
    let total = selected.len();

    if total == 0 {
        match output {
            OutputMode::Json => print_json_event(vec![
                json_str_field("event", "summary"),
                json_str_field("command", verb),
                json_usize_field("rolled_back", 0),
                json_f64_field("duration_seconds", total_started_at.elapsed().as_secs_f64()),
            ]),
            _ => {
                if matches!(target, RollbackTarget::ToVersion(_)) {
                    println!("No migrations need to be rolled back.");
                } else {
                    println!("No applied migrations to {verb}.");
                }
            }
        }
        return Ok(());
    }

    match output {
        OutputMode::Human => println!("Running {total} rollback migration(s)..."),
        OutputMode::Json => print_json_event(vec![
            json_str_field("event", "start"),
            json_str_field("command", verb),
            json_usize_field("total", total),
        ]),
        OutputMode::Quiet => {}
    }

    let mut rolled_back = 0usize;

    for applied_migration in selected {
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

        let current = rolled_back + 1;
        match output {
            OutputMode::Human => println!(
                "[{current}/{total}] Rolling back {} ({})...",
                migration.version, migration.name
            ),
            OutputMode::Json => print_json_event(vec![
                json_str_field("event", "migration_started"),
                json_str_field("command", verb),
                json_usize_field("index", current),
                json_usize_field("total", total),
                json_str_field("version", &migration.version),
                json_str_field("name", &migration.name),
            ]),
            OutputMode::Quiet => {}
        }
        let started_at = Instant::now();
        rollback_migration(&mut conn, &migration_table, migration).await?;
        rolled_back += 1;
        match output {
            OutputMode::Human => println!(
                "[{current}/{total}] Done {} ({}) in {:.2}s",
                migration.version,
                migration.name,
                started_at.elapsed().as_secs_f64()
            ),
            OutputMode::Json => print_json_event(vec![
                json_str_field("event", "migration_finished"),
                json_str_field("command", verb),
                json_usize_field("index", current),
                json_usize_field("total", total),
                json_str_field("version", &migration.version),
                json_str_field("name", &migration.name),
                json_f64_field("duration_seconds", started_at.elapsed().as_secs_f64()),
            ]),
            OutputMode::Quiet => {}
        }
    }

    match output {
        OutputMode::Json => print_json_event(vec![
            json_str_field("event", "summary"),
            json_str_field("command", verb),
            json_usize_field("rolled_back", rolled_back),
            json_f64_field("duration_seconds", total_started_at.elapsed().as_secs_f64()),
        ]),
        _ => {
            println!("Completed {verb}: reverted {rolled_back} migration(s).");
            println!("Total {verb} time: {:.2}s", total_started_at.elapsed().as_secs_f64());
        }
    }

    Ok(())
}

fn select_rollbacks(
    applied: &[AppliedMigration],
    target: &RollbackTarget,
) -> Result<Vec<AppliedMigration>> {
    match target {
        RollbackTarget::Steps(steps) => Ok(applied.iter().take(*steps).cloned().collect()),
        RollbackTarget::All => Ok(applied.to_vec()),
        RollbackTarget::ToVersion(version) => {
            let Some(index) = applied.iter().position(|item| item.version == *version) else {
                bail!("target version `{version}` is not currently applied");
            };

            Ok(applied.iter().take(index).cloned().collect())
        }
    }
}

async fn status(dir: Option<PathBuf>, database_url: &str, output: OutputMode, wait: WaitOptions) -> Result<()> {
    let migrations = load_migrations(dir)?;
    let backend = detect_backend(database_url)?;
    let mut conn = connect(database_url, output, wait, "status").await?;
    let migration_table = resolve_migration_table_name()?;

    ensure_migration_table(&mut conn, &migration_table, backend).await?;
    let applied = load_applied_migrations(&mut conn, &migration_table).await?;

    if migrations.is_empty() && applied.is_empty() {
        match output {
            OutputMode::Json => print_json_event(vec![
                json_str_field("event", "summary"),
                json_str_field("command", "status"),
                json_usize_field("count", 0),
            ]),
            _ => println!("No migrations found."),
        }
        return Ok(());
    }

    let mut emitted = 0usize;

    for migration in &migrations {
        let state = match applied.get(&migration.version) {
            Some(checksum) if checksum == &migration.checksum => "APPLIED",
            Some(_) => "APPLIED_MODIFIED",
            None => "PENDING",
        };

        match output {
            OutputMode::Json => print_json_event(vec![
                json_str_field("event", "migration"),
                json_str_field("command", "status"),
                json_str_field("state", state),
                json_str_field("version", &migration.version),
                json_str_field("name", &migration.name),
            ]),
            _ => println!("{state}\t{}\t{}", migration.version, migration.name),
        }
        emitted += 1;
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
        match output {
            OutputMode::Json => print_json_event(vec![
                json_str_field("event", "migration"),
                json_str_field("command", "status"),
                json_str_field("state", "MISSING_FILE"),
                json_str_field("version", &version),
                json_str_field("name", "<applied but no local file>"),
            ]),
            _ => println!("MISSING_FILE\t{version}\t<applied but no local file>"),
        }
        emitted += 1;
    }

    if matches!(output, OutputMode::Json) {
        print_json_event(vec![
            json_str_field("event", "summary"),
            json_str_field("command", "status"),
            json_usize_field("count", emitted),
        ]);
    }

    Ok(())
}

async fn show(database_url: &str, limit: usize, output: OutputMode, wait: WaitOptions) -> Result<()> {
    if limit == 0 {
        bail!("show limit must be at least 1");
    }

    let mut conn = connect(database_url, output, wait, "show").await?;

    match detect_backend(database_url)? {
        DatabaseBackend::Postgres => show_postgres(&mut conn, limit, output).await?,
        DatabaseBackend::MySql => show_mysql(&mut conn, limit, output).await?,
        DatabaseBackend::Sqlite => show_sqlite(&mut conn, limit, output).await?,
    }

    Ok(())
}

async fn table(name: &str, database_url: &str, output: OutputMode, wait: WaitOptions) -> Result<()> {
    let table = parse_qualified_name(name)?;
    let mut conn = connect(database_url, output, wait, "table").await?;

    match detect_backend(database_url)? {
        DatabaseBackend::Postgres => show_postgres_table(&mut conn, &table, output).await?,
        DatabaseBackend::MySql => show_mysql_table(&mut conn, &table, output).await?,
        DatabaseBackend::Sqlite => show_sqlite_table(&mut conn, &table, output).await?,
    }

    Ok(())
}

async fn show_postgres(conn: &mut AnyConnection, limit: usize, output: OutputMode) -> Result<()> {
    let row = sqlx::query(
        "SELECT current_database()::text AS database_name, current_schema()::text AS schema_name, pg_database_size(current_database()) AS database_size_bytes"
    )
    .fetch_one(&mut *conn)
    .await
    .context("failed to load PostgreSQL database info")?;

    let database_name: String = row.try_get("database_name")?;
    let schema_name: String = row.try_get("schema_name")?;
    let database_size_bytes: i64 = row.try_get("database_size_bytes")?;

    let row = sqlx::query(
        "SELECT numbackends AS open_connections FROM pg_stat_database WHERE datname = current_database()"
    )
    .fetch_optional(&mut *conn)
    .await
    .context("failed to load PostgreSQL connection info")?;
    let open_connections = row
        .and_then(|row| row.try_get::<i32, _>("open_connections").ok())
        .unwrap_or(0);

    let schemas = sqlx::query(
        "SELECT schema_name::text AS schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('pg_catalog', 'information_schema') ORDER BY schema_name"
    )
    .fetch_all(&mut *conn)
    .await
    .context("failed to load PostgreSQL schemas")?;
    let schemas = schemas
        .into_iter()
        .filter_map(|row| row.try_get::<String, _>("schema_name").ok())
        .collect::<Vec<_>>();

    let tables = sqlx::query(&format!(
        "SELECT schemaname::text AS schemaname, relname::text AS relname, pg_total_relation_size(relid) AS total_bytes \
         FROM pg_catalog.pg_statio_user_tables \
         ORDER BY total_bytes DESC, schemaname, relname \
         LIMIT {limit}"
    ))
    .fetch_all(&mut *conn)
    .await
    .context("failed to load PostgreSQL table sizes")?;

    let tables = tables
        .into_iter()
        .map(|row| -> Result<TableSize> {
            let schema: String = row.try_get("schemaname")?;
            let table: String = row.try_get("relname")?;
            let size_bytes: i64 = row.try_get("total_bytes")?;
            Ok(TableSize {
                name: format!("{schema}.{table}"),
                size_bytes: Some(size_bytes),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    match output {
        OutputMode::Json => {
            print_json_event(vec![
                json_str_field("event", "database_info"),
                json_str_field("command", "show"),
                json_str_field("backend", "postgres"),
                json_str_field("database", &database_name),
                json_str_field("current_schema", &schema_name),
                json_i64_field("database_size_bytes", database_size_bytes),
                json_i64_field("open_connections", i64::from(open_connections)),
                json_string_array_field("schemas", &schemas),
            ]);
            for table in &tables {
                print_json_event(vec![
                    json_str_field("event", "table_size"),
                    json_str_field("command", "show"),
                    json_str_field("name", &table.name),
                    json_i64_field("size_bytes", table.size_bytes.unwrap_or(0)),
                ]);
            }
            print_json_event(vec![
                json_str_field("event", "summary"),
                json_str_field("command", "show"),
                json_usize_field("count", tables.len()),
            ]);
        }
        _ => {
            println!("Backend: postgres");
            println!("Database: {database_name}");
            println!("Current schema: {schema_name}");
            println!("Database size: {}", humanize_bytes(database_size_bytes));
            println!("Open connections: {open_connections}");
            println!(
                "Schemas: {}",
                if schemas.is_empty() {
                    "<none>".to_string()
                } else {
                    schemas.join(", ")
                }
            );
            print_table_sizes("Largest tables", &tables);
        }
    }

    Ok(())
}

async fn show_postgres_table(conn: &mut AnyConnection, table: &QualifiedName, output: OutputMode) -> Result<()> {
    let row =
        sqlx::query("SELECT current_database()::text AS database_name, current_schema()::text AS schema_name")
            .fetch_one(&mut *conn)
            .await
            .context("failed to load PostgreSQL database info")?;
    let database_name: String = row.try_get("database_name")?;
    let current_schema: String = row.try_get("schema_name")?;
    let schema = table.schema.clone().unwrap_or(current_schema);

    let row = sqlx::query(
        "SELECT c.reltuples::bigint AS estimated_rows, pg_total_relation_size(c.oid) AS size_bytes \
         FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind = 'r'",
    )
    .bind(&schema)
    .bind(&table.name)
    .fetch_optional(&mut *conn)
    .await
    .context("failed to load PostgreSQL table stats")?
    .ok_or_else(|| anyhow!("table `{schema}.{}` was not found", table.name))?;
    let estimated_rows: i64 = row.try_get("estimated_rows")?;
    let size_bytes: i64 = row.try_get("size_bytes")?;

    let columns = sqlx::query(
        "SELECT column_name::text AS column_name, data_type, is_nullable, column_default \
         FROM information_schema.columns \
         WHERE table_schema = $1 AND table_name = $2 \
         ORDER BY ordinal_position",
    )
    .bind(&schema)
    .bind(&table.name)
    .fetch_all(&mut *conn)
    .await
    .context("failed to load PostgreSQL columns")?;

    let columns = columns
        .into_iter()
        .map(|row| -> Result<ColumnInfo> {
            Ok(ColumnInfo {
                name: row.try_get("column_name")?,
                data_type: row.try_get("data_type")?,
                nullable: row.try_get::<String, _>("is_nullable")? == "YES",
                default_value: row.try_get("column_default")?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let indexes = sqlx::query(
        "SELECT indexname::text AS indexname, indexdef \
         FROM pg_indexes \
         WHERE schemaname = $1 AND tablename = $2 \
         ORDER BY indexname",
    )
    .bind(&schema)
    .bind(&table.name)
    .fetch_all(&mut *conn)
    .await
    .context("failed to load PostgreSQL indexes")?;

    let indexes = indexes
        .into_iter()
        .map(|row| -> Result<IndexInfo> {
            let name: String = row.try_get("indexname")?;
            let definition: String = row.try_get("indexdef")?;
            Ok(IndexInfo {
                unique: definition.contains("UNIQUE INDEX"),
                columns: extract_index_columns(&definition),
                name,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    match output {
        OutputMode::Json => {
            print_json_event(vec![
                json_str_field("event", "table_info"),
                json_str_field("command", "table"),
                json_str_field("backend", "postgres"),
                json_str_field("database", &database_name),
                json_str_field("table", &format!("{schema}.{}", table.name)),
                json_i64_field("estimated_rows", estimated_rows),
                json_i64_field("size_bytes", size_bytes),
            ]);
            for column in &columns {
                let mut fields = vec![
                    json_str_field("event", "column"),
                    json_str_field("command", "table"),
                    json_str_field("table", &format!("{schema}.{}", table.name)),
                    json_str_field("name", &column.name),
                    json_str_field("data_type", &column.data_type),
                    json_bool_field("nullable", column.nullable),
                ];
                if let Some(default_value) = &column.default_value {
                    fields.push(json_str_field("default", default_value));
                } else {
                    fields.push(json_null_field("default"));
                }
                print_json_event(fields);
            }
            for index in &indexes {
                print_json_event(vec![
                    json_str_field("event", "index"),
                    json_str_field("command", "table"),
                    json_str_field("table", &format!("{schema}.{}", table.name)),
                    json_str_field("name", &index.name),
                    json_bool_field("unique", index.unique),
                    json_string_array_field("columns", &index.columns),
                ]);
            }
            print_json_event(vec![
                json_str_field("event", "summary"),
                json_str_field("command", "table"),
                json_usize_field("columns", columns.len()),
                json_usize_field("indexes", indexes.len()),
            ]);
        }
        _ => {
            println!("Backend: postgres");
            println!("Database: {database_name}");
            println!("Table: {schema}.{}", table.name);
            println!("Estimated rows: {estimated_rows}");
            println!("Table size: {}", humanize_bytes(size_bytes));
            print_columns(&columns);
            print_indexes(&indexes);
        }
    }

    Ok(())
}

async fn show_mysql(conn: &mut AnyConnection, limit: usize, output: OutputMode) -> Result<()> {
    let row = sqlx::query(
        "SELECT DATABASE() AS database_name, \
         CAST(COALESCE(SUM(data_length + index_length), 0) AS SIGNED) AS database_size_bytes \
         FROM information_schema.tables \
         WHERE table_schema = DATABASE()",
    )
    .fetch_one(&mut *conn)
    .await
    .context("failed to load MySQL database info")?;

    let database_name: Option<String> = row.try_get("database_name")?;
    let database_name = database_name
        .ok_or_else(|| anyhow!("no MySQL database is selected for this connection"))?;
    let database_size_bytes: i64 = row.try_get("database_size_bytes")?;

    let row = sqlx::query(
        "SELECT COUNT(*) AS open_connections FROM information_schema.processlist WHERE db = DATABASE()"
    )
    .fetch_one(&mut *conn)
    .await
    .context("failed to load MySQL connection info")?;
    let open_connections: i64 = row.try_get("open_connections")?;

    let tables = sqlx::query(&format!(
        "SELECT table_name, CAST(COALESCE(data_length + index_length, 0) AS SIGNED) AS total_bytes \
         FROM information_schema.tables \
         WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE' \
         ORDER BY total_bytes DESC, table_name \
         LIMIT {limit}"
    ))
    .fetch_all(&mut *conn)
    .await
    .context("failed to load MySQL table sizes")?;

    let tables = tables
        .into_iter()
        .map(|row| -> Result<TableSize> {
            Ok(TableSize {
                name: row.try_get("table_name")?,
                size_bytes: Some(row.try_get("total_bytes")?),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    match output {
        OutputMode::Json => {
            print_json_event(vec![
                json_str_field("event", "database_info"),
                json_str_field("command", "show"),
                json_str_field("backend", "mysql"),
                json_str_field("database", &database_name),
                json_i64_field("database_size_bytes", database_size_bytes),
                json_i64_field("open_connections", open_connections),
            ]);
            for table in &tables {
                print_json_event(vec![
                    json_str_field("event", "table_size"),
                    json_str_field("command", "show"),
                    json_str_field("name", &table.name),
                    json_i64_field("size_bytes", table.size_bytes.unwrap_or(0)),
                ]);
            }
            print_json_event(vec![
                json_str_field("event", "summary"),
                json_str_field("command", "show"),
                json_usize_field("count", tables.len()),
            ]);
        }
        _ => {
            println!("Backend: mysql");
            println!("Database: {database_name}");
            println!("Database size: {}", humanize_bytes(database_size_bytes));
            println!("Open connections: {open_connections}");
            print_table_sizes("Largest tables", &tables);
        }
    }

    Ok(())
}

async fn show_mysql_table(conn: &mut AnyConnection, table: &QualifiedName, output: OutputMode) -> Result<()> {
    let database_name = table.schema.clone().unwrap_or_else(|| "".to_string());

    let database_name = if database_name.is_empty() {
        let row = sqlx::query("SELECT DATABASE() AS database_name")
            .fetch_one(&mut *conn)
            .await
            .context("failed to load MySQL database info")?;
        let database_name: Option<String> = row.try_get("database_name")?;
        database_name.ok_or_else(|| anyhow!("no MySQL database is selected for this connection"))?
    } else {
        database_name
    };

    let row = sqlx::query(
        "SELECT CAST(COALESCE(table_rows, 0) AS SIGNED) AS table_rows, CAST(COALESCE(data_length + index_length, 0) AS SIGNED) AS size_bytes \
         FROM information_schema.tables \
         WHERE table_schema = ? AND table_name = ? AND table_type = 'BASE TABLE'"
    )
    .bind(&database_name)
    .bind(&table.name)
    .fetch_optional(&mut *conn)
    .await
    .context("failed to load MySQL table stats")?
    .ok_or_else(|| anyhow!("table `{database_name}.{}` was not found", table.name))?;
    let table_rows: i64 = row.try_get("table_rows")?;
    let size_bytes: i64 = row.try_get("size_bytes")?;

    let columns = sqlx::query(
        "SELECT CONCAT(column_name) AS column_name, CONCAT(column_type) AS column_type, is_nullable, IF(column_default IS NULL, NULL, CONCAT(column_default)) AS column_default \
         FROM information_schema.columns \
         WHERE table_schema = ? AND table_name = ? \
         ORDER BY ordinal_position",
    )
    .bind(&database_name)
    .bind(&table.name)
    .fetch_all(&mut *conn)
    .await
    .context("failed to load MySQL columns")?;

    let columns = columns
        .into_iter()
        .map(|row| -> Result<ColumnInfo> {
            Ok(ColumnInfo {
                name: get_string_like(&row, "column_name")?,
                data_type: get_string_like(&row, "column_type")?,
                nullable: row.try_get::<String, _>("is_nullable")? == "YES",
                default_value: row
                    .try_get::<Option<String>, _>("column_default")
                    .ok()
                    .flatten()
                    .or_else(|| {
                        row.try_get::<Option<Vec<u8>>, _>("column_default")
                            .ok()
                            .flatten()
                            .and_then(|bytes| String::from_utf8(bytes).ok())
                    }),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let indexes = sqlx::query(
        "SELECT CONCAT(index_name) AS index_name, non_unique, seq_in_index, CONCAT(column_name) AS column_name \
         FROM information_schema.statistics \
         WHERE table_schema = ? AND table_name = ? \
         ORDER BY index_name, seq_in_index",
    )
    .bind(&database_name)
    .bind(&table.name)
    .fetch_all(&mut *conn)
    .await
    .context("failed to load MySQL indexes")?;
    let indexes = group_index_rows(indexes, "index_name", "column_name", "non_unique")?;

    match output {
        OutputMode::Json => {
            let table_name = format!("{database_name}.{}", table.name);
            print_json_event(vec![
                json_str_field("event", "table_info"),
                json_str_field("command", "table"),
                json_str_field("backend", "mysql"),
                json_str_field("database", &database_name),
                json_str_field("table", &table_name),
                json_i64_field("estimated_rows", table_rows),
                json_i64_field("size_bytes", size_bytes),
            ]);
            for column in &columns {
                let mut fields = vec![
                    json_str_field("event", "column"),
                    json_str_field("command", "table"),
                    json_str_field("table", &table_name),
                    json_str_field("name", &column.name),
                    json_str_field("data_type", &column.data_type),
                    json_bool_field("nullable", column.nullable),
                ];
                if let Some(default_value) = &column.default_value {
                    fields.push(json_str_field("default", default_value));
                } else {
                    fields.push(json_null_field("default"));
                }
                print_json_event(fields);
            }
            for index in &indexes {
                print_json_event(vec![
                    json_str_field("event", "index"),
                    json_str_field("command", "table"),
                    json_str_field("table", &table_name),
                    json_str_field("name", &index.name),
                    json_bool_field("unique", index.unique),
                    json_string_array_field("columns", &index.columns),
                ]);
            }
            print_json_event(vec![
                json_str_field("event", "summary"),
                json_str_field("command", "table"),
                json_usize_field("columns", columns.len()),
                json_usize_field("indexes", indexes.len()),
            ]);
        }
        _ => {
            println!("Backend: mysql");
            println!("Database: {database_name}");
            println!("Table: {database_name}.{}", table.name);
            println!("Estimated rows: {table_rows}");
            println!("Table size: {}", humanize_bytes(size_bytes));
            print_columns(&columns);
            print_indexes(&indexes);
        }
    }

    Ok(())
}

async fn show_sqlite(conn: &mut AnyConnection, limit: usize, output: OutputMode) -> Result<()> {
    let row = sqlx::query("PRAGMA page_count")
        .fetch_one(&mut *conn)
        .await
        .context("failed to load SQLite page count")?;
    let page_count: i64 = row.try_get(0)?;

    let row = sqlx::query("PRAGMA page_size")
        .fetch_one(&mut *conn)
        .await
        .context("failed to load SQLite page size")?;
    let page_size: i64 = row.try_get(0)?;

    let db_list = sqlx::query("PRAGMA database_list")
        .fetch_all(&mut *conn)
        .await
        .context("failed to load SQLite database list")?;
    let database_path = db_list
        .into_iter()
        .find_map(|row| row.try_get::<String, _>("file").ok())
        .filter(|path| !path.is_empty())
        .unwrap_or_else(|| ":memory:".to_string());

    let tables = sqlx::query(&format!(
        "SELECT name FROM sqlite_master \
         WHERE type = 'table' AND name NOT LIKE 'sqlite_%' \
         ORDER BY name \
         LIMIT {limit}"
    ))
    .fetch_all(&mut *conn)
    .await
    .context("failed to load SQLite tables")?;

    let tables = tables
        .into_iter()
        .map(|row| -> Result<TableSize> {
            Ok(TableSize {
                name: row.try_get("name")?,
                size_bytes: None,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    match output {
        OutputMode::Json => {
            print_json_event(vec![
                json_str_field("event", "database_info"),
                json_str_field("command", "show"),
                json_str_field("backend", "sqlite"),
                json_str_field("database", &database_path),
                json_i64_field("database_size_bytes", page_count * page_size),
            ]);
            for table in &tables {
                print_json_event(vec![
                    json_str_field("event", "table_size"),
                    json_str_field("command", "show"),
                    json_str_field("name", &table.name),
                    json_null_field("size_bytes"),
                ]);
            }
            print_json_event(vec![
                json_str_field("event", "summary"),
                json_str_field("command", "show"),
                json_usize_field("count", tables.len()),
            ]);
        }
        _ => {
            println!("Backend: sqlite");
            println!("Database: {database_path}");
            println!("Database size: {}", humanize_bytes(page_count * page_size));
            println!("Open connections: n/a");
            print_table_sizes("Tables", &tables);
        }
    }

    Ok(())
}

async fn show_sqlite_table(conn: &mut AnyConnection, table: &QualifiedName, output: OutputMode) -> Result<()> {
    if table.schema.is_some() {
        bail!("sqlite table inspection does not support schema-qualified names");
    }

    let quoted = quote_identifier(&table.name, DatabaseBackend::Sqlite);

    let row = sqlx::query(&format!("SELECT COUNT(*) AS row_count FROM {quoted}"))
        .fetch_optional(&mut *conn)
        .await
        .with_context(|| format!("failed to inspect SQLite table `{}`", table.name))?
        .ok_or_else(|| anyhow!("table `{}` was not found", table.name))?;
    let row_count: i64 = row.try_get("row_count")?;

    let columns = sqlx::query(&format!("PRAGMA table_info({quoted})"))
        .fetch_all(&mut *conn)
        .await
        .context("failed to load SQLite columns")?;
    let columns = columns
        .into_iter()
        .map(|row| -> Result<ColumnInfo> {
            Ok(ColumnInfo {
                name: row.try_get("name")?,
                data_type: row.try_get("type")?,
                nullable: row.try_get::<i64, _>("notnull")? == 0,
                default_value: row.try_get("dflt_value")?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let index_rows = sqlx::query(&format!("PRAGMA index_list({quoted})"))
        .fetch_all(&mut *conn)
        .await
        .context("failed to load SQLite indexes")?;

    let mut indexes = Vec::with_capacity(index_rows.len());
    for row in index_rows {
        let index_name: String = row.try_get("name")?;
        let unique = row.try_get::<i64, _>("unique")? == 1;
        let index_quoted = quote_identifier(&index_name, DatabaseBackend::Sqlite);
        let column_rows = sqlx::query(&format!("PRAGMA index_info({index_quoted})"))
            .fetch_all(&mut *conn)
            .await
            .with_context(|| format!("failed to load SQLite index columns for `{index_name}`"))?;
        let columns = column_rows
            .into_iter()
            .filter_map(|row| row.try_get::<String, _>("name").ok())
            .collect::<Vec<_>>();
        indexes.push(IndexInfo {
            name: index_name,
            columns,
            unique,
        });
    }

    match output {
        OutputMode::Json => {
            print_json_event(vec![
                json_str_field("event", "table_info"),
                json_str_field("command", "table"),
                json_str_field("backend", "sqlite"),
                json_str_field("table", &table.name),
                json_i64_field("rows", row_count),
                json_null_field("size_bytes"),
            ]);
            for column in &columns {
                let mut fields = vec![
                    json_str_field("event", "column"),
                    json_str_field("command", "table"),
                    json_str_field("table", &table.name),
                    json_str_field("name", &column.name),
                    json_str_field("data_type", &column.data_type),
                    json_bool_field("nullable", column.nullable),
                ];
                if let Some(default_value) = &column.default_value {
                    fields.push(json_str_field("default", default_value));
                } else {
                    fields.push(json_null_field("default"));
                }
                print_json_event(fields);
            }
            for index in &indexes {
                print_json_event(vec![
                    json_str_field("event", "index"),
                    json_str_field("command", "table"),
                    json_str_field("table", &table.name),
                    json_str_field("name", &index.name),
                    json_bool_field("unique", index.unique),
                    json_string_array_field("columns", &index.columns),
                ]);
            }
            print_json_event(vec![
                json_str_field("event", "summary"),
                json_str_field("command", "table"),
                json_usize_field("columns", columns.len()),
                json_usize_field("indexes", indexes.len()),
            ]);
        }
        _ => {
            println!("Backend: sqlite");
            println!("Table: {}", table.name);
            println!("Rows: {row_count}");
            println!("Table size: n/a");
            print_columns(&columns);
            print_indexes(&indexes);
        }
    }

    Ok(())
}

fn print_table_sizes(title: &str, tables: &[TableSize]) {
    println!("{title}:");
    if tables.is_empty() {
        println!("  <none>");
        return;
    }

    for table in tables {
        match table.size_bytes {
            Some(size_bytes) => println!("  {} ({})", table.name, humanize_bytes(size_bytes)),
            None => println!("  {}", table.name),
        }
    }
}

fn print_columns(columns: &[ColumnInfo]) {
    println!("Columns:");
    if columns.is_empty() {
        println!("  <none>");
        return;
    }

    for column in columns {
        let nullable = if column.nullable { "NULL" } else { "NOT NULL" };
        match &column.default_value {
            Some(default_value) => println!(
                "  {} {} {} DEFAULT {}",
                column.name, column.data_type, nullable, default_value
            ),
            None => println!("  {} {} {}", column.name, column.data_type, nullable),
        }
    }
}

fn print_indexes(indexes: &[IndexInfo]) {
    println!("Indexes:");
    if indexes.is_empty() {
        println!("  <none>");
        return;
    }

    for index in indexes {
        let unique = if index.unique { "UNIQUE " } else { "" };
        let columns = if index.columns.is_empty() {
            "<unknown>".to_string()
        } else {
            index.columns.join(", ")
        };
        println!("  {}{} ({})", unique, index.name, columns);
    }
}

fn humanize_bytes(bytes: i64) -> String {
    let mut value = bytes.max(0) as f64;
    let units = ["B", "KB", "MB", "GB", "TB", "PB"];
    let mut unit = 0usize;

    while value >= 1024.0 && unit < units.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }

    if unit == 0 {
        format!("{} {}", value as u64, units[unit])
    } else {
        format!("{value:.1} {}", units[unit])
    }
}

fn parse_qualified_name(name: &str) -> Result<QualifiedName> {
    let parts = name.split('.').collect::<Vec<_>>();
    match parts.as_slice() {
        [table] => {
            validate_identifier(table)?;
            Ok(QualifiedName {
                schema: None,
                name: (*table).to_string(),
            })
        }
        [schema, table] => {
            validate_identifier(schema)?;
            validate_identifier(table)?;
            Ok(QualifiedName {
                schema: Some((*schema).to_string()),
                name: (*table).to_string(),
            })
        }
        _ => bail!("table name must be `table` or `schema.table`"),
    }
}

fn extract_index_columns(definition: &str) -> Vec<String> {
    let Some(start) = definition.find('(') else {
        return Vec::new();
    };
    let Some(end) = definition.rfind(')') else {
        return Vec::new();
    };
    definition[start + 1..end]
        .split(',')
        .map(|column| column.trim().trim_matches('"').to_string())
        .filter(|column| !column.is_empty())
        .collect()
}

fn get_string_like(row: &sqlx::any::AnyRow, field: &str) -> Result<String> {
    if let Ok(value) = row.try_get::<String, _>(field) {
        return Ok(value);
    }

    if let Ok(bytes) = row.try_get::<Vec<u8>, _>(field) {
        return String::from_utf8(bytes)
            .with_context(|| format!("failed to decode utf-8 for column `{field}`"));
    }

    bail!("failed to decode string column `{field}`")
}

fn group_index_rows(
    rows: Vec<sqlx::any::AnyRow>,
    index_name_field: &str,
    column_name_field: &str,
    non_unique_field: &str,
) -> Result<Vec<IndexInfo>> {
    let mut indexes: Vec<IndexInfo> = Vec::new();
    let mut by_name: HashMap<String, usize> = HashMap::new();

    for row in rows {
        let name = get_string_like(&row, index_name_field)?;
        let column = get_string_like(&row, column_name_field)?;
        let unique = row.try_get::<i64, _>(non_unique_field)? == 0;

        if let Some(index) = by_name.get(&name).copied() {
            indexes[index].columns.push(column);
        } else {
            by_name.insert(name.clone(), indexes.len());
            indexes.push(IndexInfo {
                name,
                columns: vec![column],
                unique,
            });
        }
    }

    Ok(indexes)
}

async fn wipe(database_url: &str, yes: bool, output: OutputMode, wait: WaitOptions) -> Result<()> {
    if !yes {
        bail!("wipe is destructive; re-run with `--yes` to confirm");
    }

    let total_started_at = Instant::now();
    match output {
        OutputMode::Human => println!("[1/1] Wiping database..."),
        OutputMode::Json => print_json_event(vec![
            json_str_field("event", "start"),
            json_str_field("command", "wipe"),
        ]),
        OutputMode::Quiet => {}
    }
    let started_at = Instant::now();
    let mut conn = connect(database_url, output, wait, "wipe").await?;

    match detect_backend(database_url)? {
        DatabaseBackend::Postgres => wipe_postgres(&mut conn).await?,
        DatabaseBackend::MySql => wipe_mysql(&mut conn).await?,
        DatabaseBackend::Sqlite => wipe_sqlite(&mut conn).await?,
    }

    match output {
        OutputMode::Human => {
            println!("[1/1] Done wiping database in {:.2}s", started_at.elapsed().as_secs_f64());
            println!("Database wiped.");
            println!("Total wipe time: {:.2}s", total_started_at.elapsed().as_secs_f64());
        }
        OutputMode::Json => print_json_event(vec![
            json_str_field("event", "summary"),
            json_str_field("command", "wipe"),
            json_f64_field("duration_seconds", total_started_at.elapsed().as_secs_f64()),
        ]),
        OutputMode::Quiet => {
            println!("Database wiped.");
            println!("Total wipe time: {:.2}s", total_started_at.elapsed().as_secs_f64());
        }
    }
    Ok(())
}

async fn fresh(dir: Option<PathBuf>, database_url: &str, yes: bool, output: OutputMode, wait: WaitOptions) -> Result<()> {
    if !yes {
        bail!("fresh is destructive; re-run with `--yes` to confirm");
    }

    let total_started_at = Instant::now();
    match output {
        OutputMode::Human => println!("[1/2] Wiping database..."),
        OutputMode::Json => print_json_event(vec![
            json_str_field("event", "start"),
            json_str_field("command", "fresh"),
        ]),
        OutputMode::Quiet => {}
    }
    let wipe_started_at = Instant::now();
    let mut conn = connect(database_url, output, wait, "fresh").await?;

    match detect_backend(database_url)? {
        DatabaseBackend::Postgres => wipe_postgres(&mut conn).await?,
        DatabaseBackend::MySql => wipe_mysql(&mut conn).await?,
        DatabaseBackend::Sqlite => wipe_sqlite(&mut conn).await?,
    }

    match output {
        OutputMode::Human => {
            println!("[1/2] Done wiping database in {:.2}s", wipe_started_at.elapsed().as_secs_f64());
            println!("[2/2] Running migrations...");
        }
        OutputMode::Json => print_json_event(vec![
            json_str_field("event", "phase_finished"),
            json_str_field("command", "fresh"),
            json_str_field("phase", "wipe"),
            json_f64_field("duration_seconds", wipe_started_at.elapsed().as_secs_f64()),
        ]),
        OutputMode::Quiet => {}
    }
    let migrate_started_at = Instant::now();
    migrate(dir, database_url, output, wait).await?;
    match output {
        OutputMode::Human => {
            println!("[2/2] Done running migrations in {:.2}s", migrate_started_at.elapsed().as_secs_f64());
            println!("Total fresh time: {:.2}s", total_started_at.elapsed().as_secs_f64());
        }
        OutputMode::Json => print_json_event(vec![
            json_str_field("event", "phase_finished"),
            json_str_field("command", "fresh"),
            json_str_field("phase", "migrate"),
            json_f64_field("duration_seconds", migrate_started_at.elapsed().as_secs_f64()),
        ]),
        OutputMode::Quiet => println!("Total fresh time: {:.2}s", total_started_at.elapsed().as_secs_f64()),
    }
    if matches!(output, OutputMode::Json) {
        print_json_event(vec![
            json_str_field("event", "summary"),
            json_str_field("command", "fresh"),
            json_f64_field("duration_seconds", total_started_at.elapsed().as_secs_f64()),
        ]);
    }
    Ok(())
}

async fn connect(
    database_url: &str,
    output: OutputMode,
    wait: WaitOptions,
    command: &str,
) -> Result<AnyConnection> {
    let normalized_url = normalize_database_url(database_url);

    if !wait.enabled {
        return AnyConnection::connect(&normalized_url)
            .await
            .with_context(|| format!("failed to connect to database at {database_url}"));
    }

    match output {
        OutputMode::Human => println!(
            "Waiting for database readiness (timeout: {:.0}s, interval: {:.0}s)...",
            wait.timeout.as_secs_f64(),
            wait.interval.as_secs_f64()
        ),
        OutputMode::Json => print_json_event(vec![
            json_str_field("event", "waiting_for_database"),
            json_str_field("command", command),
            json_f64_field("timeout_seconds", wait.timeout.as_secs_f64()),
            json_f64_field("interval_seconds", wait.interval.as_secs_f64()),
        ]),
        OutputMode::Quiet => {}
    }

    let started_at = Instant::now();
    loop {
        match AnyConnection::connect(&normalized_url).await {
            Ok(conn) => {
                match output {
                    OutputMode::Human => println!(
                        "Database ready after {:.2}s.",
                        started_at.elapsed().as_secs_f64()
                    ),
                    OutputMode::Json => print_json_event(vec![
                        json_str_field("event", "database_ready"),
                        json_str_field("command", command),
                        json_f64_field("waited_seconds", started_at.elapsed().as_secs_f64()),
                    ]),
                    OutputMode::Quiet => {}
                }
                return Ok(conn);
            }
            Err(err) => {
                if started_at.elapsed() >= wait.timeout {
                    return Err(err)
                        .with_context(|| format!("timed out waiting for database at {database_url}"));
                }
                tokio::time::sleep(wait.interval).await;
            }
        }
    }
}

fn normalize_database_url(database_url: &str) -> String {
    if let Some(rest) = database_url.strip_prefix("pgsql://") {
        return format!("postgres://{rest}");
    }

    database_url.to_owned()
}

fn detect_backend(database_url: &str) -> Result<DatabaseBackend> {
    if database_url.starts_with("postgres://")
        || database_url.starts_with("postgresql://")
        || database_url.starts_with("pgsql://")
    {
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
    let row = sqlx::query("SELECT current_schema()::text AS schema_name")
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

async fn ensure_migration_table(
    conn: &mut AnyConnection,
    migration_table: &str,
    backend: DatabaseBackend,
) -> Result<()> {
    match backend {
        DatabaseBackend::Postgres => {
            let exists = sqlx::query(
                "SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = $1"
            )
            .bind(migration_table)
            .fetch_optional(&mut *conn)
            .await
            .context("failed to check migration tracking table")?
            .is_some();

            if !exists {
                let sql = format!(
                    "CREATE TABLE {migration_table} (
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
            }
        }
        DatabaseBackend::MySql | DatabaseBackend::Sqlite => {
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
        }
    }

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

    let sql = migration_record_insert_sql(migration_table, migration);

    sqlx::raw_sql(&sql)
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

    let sql = migration_record_delete_sql(migration_table, &migration.version);

    sqlx::raw_sql(&sql).execute(&mut *tx).await.with_context(|| {
        format!(
            "failed to remove migration record {}",
            migration.path.display()
        )
    })?;

    tx.commit().await.context("failed to commit rollback")?;
    Ok(())
}

fn escape_sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}

fn migration_record_insert_sql(migration_table: &str, migration: &Migration) -> String {
    format!(
        "INSERT INTO {migration_table} (version, name, checksum) VALUES ('{}', '{}', '{}')",
        escape_sql_literal(&migration.version),
        escape_sql_literal(&migration.name),
        escape_sql_literal(&migration.checksum)
    )
}

fn migration_record_delete_sql(migration_table: &str, version: &str) -> String {
    format!(
        "DELETE FROM {migration_table} WHERE version = '{}'",
        escape_sql_literal(version)
    )
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
            "migration filename must look like yyyy_mm_dd_hhmmss-name.sql: {}",
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
    let mut last_separator: Option<char> = None;

    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() {
            sanitized.push(ch.to_ascii_lowercase());
            last_separator = None;
        } else if matches!(ch, '_' | '-') {
            if last_separator != Some(ch) {
                sanitized.push(ch);
                last_separator = Some(ch);
            }
        } else if last_separator != Some('_') {
            sanitized.push('_');
            last_separator = Some('_');
        }
    }

    sanitized
        .trim_matches(|ch| matches!(ch, '_' | '-'))
        .to_string()
}

fn timestamp_prefix() -> String {
    let now = Local::now();
    now.format("%Y_%m_%d_%H%M%S").to_string()
}

fn checksum(content: &str) -> String {
    let digest = Sha256::digest(content.as_bytes());
    format!("{digest:x}")
}

#[cfg(test)]
mod tests {
    use super::{
        AppliedMigration, DEFAULT_MIGRATION_TABLE, DOWN_MARKER, DatabaseBackend, RollbackTarget,
        UP_MARKER, detect_backend, extract_index_columns, humanize_bytes, infer_table_name,
        parse_migration_sections, parse_migration_table_name, parse_qualified_name,
        quote_identifier, resolve_env_file_override, sanitize_migration_name,
        scaffold_table_migration, select_rollbacks, validate_identifier,
    };

    #[test]
    fn sanitizes_migration_names() {
        assert_eq!(
            sanitize_migration_name("Create Users Table"),
            "create_users_table"
        );
        assert_eq!(
            sanitize_migration_name("add__email!!index"),
            "add_email_index"
        );
        assert_eq!(
            sanitize_migration_name("create-users-table"),
            "create-users-table"
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
            detect_backend("postgresql://localhost/db").expect("postgresql should parse"),
            DatabaseBackend::Postgres
        );
        assert_eq!(
            detect_backend("pgsql://localhost/db").expect("pgsql should parse"),
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

    #[test]
    fn rollback_to_version_is_exclusive() {
        let applied = vec![
            AppliedMigration {
                version: "003".to_string(),
                checksum: "c3".to_string(),
            },
            AppliedMigration {
                version: "002".to_string(),
                checksum: "c2".to_string(),
            },
            AppliedMigration {
                version: "001".to_string(),
                checksum: "c1".to_string(),
            },
        ];

        let selected = select_rollbacks(&applied, &RollbackTarget::ToVersion("002".to_string()))
            .expect("selection should succeed");

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].version, "003");
    }

    #[test]
    fn rollback_to_latest_version_selects_nothing() {
        let applied = vec![
            AppliedMigration {
                version: "003".to_string(),
                checksum: "c3".to_string(),
            },
            AppliedMigration {
                version: "002".to_string(),
                checksum: "c2".to_string(),
            },
        ];

        let selected = select_rollbacks(&applied, &RollbackTarget::ToVersion("003".to_string()))
            .expect("selection should succeed");

        assert!(selected.is_empty());
    }

    #[test]
    fn rollback_to_unknown_version_fails() {
        let applied = vec![AppliedMigration {
            version: "003".to_string(),
            checksum: "c3".to_string(),
        }];

        let err = select_rollbacks(&applied, &RollbackTarget::ToVersion("001".to_string()))
            .expect_err("selection should fail");

        assert!(err.to_string().contains("not currently applied"));
    }

    #[test]
    fn timestamp_prefix_uses_expected_format() {
        let prefix = super::timestamp_prefix();

        assert!(!prefix.contains('.'));
        assert_eq!(prefix.matches('_').count(), 3);
        assert_eq!(prefix.len(), 17);
    }

    #[test]
    fn humanizes_bytes() {
        assert_eq!(humanize_bytes(999), "999 B");
        assert_eq!(humanize_bytes(1024), "1.0 KB");
        assert_eq!(humanize_bytes(5 * 1024 * 1024), "5.0 MB");
    }

    #[test]
    fn parses_qualified_table_name() {
        let table = parse_qualified_name("public.users").expect("qualified name should parse");
        assert_eq!(table.schema.as_deref(), Some("public"));
        assert_eq!(table.name, "users");

        let table = parse_qualified_name("users").expect("simple name should parse");
        assert_eq!(table.schema, None);
        assert_eq!(table.name, "users");
    }

    #[test]
    fn rejects_invalid_qualified_table_name() {
        assert!(parse_qualified_name("bad-name").is_err());
        assert!(parse_qualified_name("a.b.c").is_err());
    }

    #[test]
    fn extracts_index_columns_from_definition() {
        let columns = extract_index_columns(
            "CREATE UNIQUE INDEX users_email_idx ON public.users USING btree (email, created_at)",
        );

        assert_eq!(columns, vec!["email".to_string(), "created_at".to_string()]);
    }
}
