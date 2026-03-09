use clap::{Parser, Subcommand};
use comfy_table::{Attribute, Cell, Table as PrettyTable};
use redb::{
    Database, MultimapTableHandle, ReadOnlyDatabase, ReadableDatabase, ReadableTableMetadata,
    TableHandle,
};
use std::io::Read;
use std::path::PathBuf;
use std::process;

#[derive(Parser)]
#[command(
    name = "redb",
    about = "Inspect, verify, and manage redb database files",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Show database header and metadata
    Info {
        /// Path to the redb database file
        path: PathBuf,
    },
    /// List all tables with row counts and storage stats
    Tables {
        /// Path to the redb database file
        path: PathBuf,
    },
    /// Show detailed database and per-table storage statistics
    Stats {
        /// Path to the redb database file
        path: PathBuf,
    },
    /// Verify database integrity (checksums + B-tree structure)
    Verify {
        /// Path to the redb database file
        path: PathBuf,
    },
    /// Compact the database file to reclaim free space
    Compact {
        /// Path to the redb database file
        path: PathBuf,
    },
    /// Dump raw key-value contents of a table
    Dump {
        /// Path to the redb database file
        path: PathBuf,
        /// Table name to dump
        #[arg(short, long)]
        table: String,
        /// Maximum number of entries to display
        #[arg(short, long, default_value_t = 100)]
        limit: usize,
        /// Show values as raw hex instead of attempting UTF-8
        #[arg(long)]
        hex: bool,
    },
    /// Inspect the raw 320-byte file header (magic, god byte, commit slots)
    Header {
        /// Path to the redb database file
        path: PathBuf,
    },
}

fn main() {
    let cli = Cli::parse();

    let result = match cli.command {
        Commands::Info { path } => cmd_info(&path),
        Commands::Tables { path } => cmd_tables(&path),
        Commands::Stats { path } => cmd_stats(&path),
        Commands::Verify { path } => cmd_verify(&path),
        Commands::Compact { path } => cmd_compact(&path),
        Commands::Dump {
            path,
            table,
            limit,
            hex,
        } => cmd_dump(&path, &table, limit, hex),
        Commands::Header { path } => cmd_header(&path),
    };

    if let Err(e) = result {
        eprintln!("error: {e}");
        process::exit(1);
    }
}

fn cmd_info(path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let file_size = std::fs::metadata(path)?.len();

    // Database stats require a write transaction (redb API limitation)
    let db = Database::open(path)?;
    let txn = db.begin_write()?;

    let table_count = txn.list_tables()?.count();
    let multimap_count = txn.list_multimap_tables()?.count();
    let stats = txn.stats()?;
    txn.abort()?;
    drop(db);

    // Read compression algorithm from raw header byte 36
    let compression_name = read_compression_name(path);

    println!("Database: {}", path.display());
    println!("File size:        {}", format_bytes(file_size));
    println!("Page size:        {} bytes", stats.page_size());
    println!("Compression:      {compression_name}");
    println!("Allocated pages:  {}", stats.allocated_pages());
    println!("Tree height:      {}", stats.tree_height());
    println!("Tables:           {table_count}");
    println!("Multimap tables:  {multimap_count}");
    println!("Stored data:      {}", format_bytes(stats.stored_bytes()));
    println!("Metadata:         {}", format_bytes(stats.metadata_bytes()));
    println!(
        "Fragmented:       {} ({:.1}%)",
        format_bytes(stats.fragmented_bytes()),
        if stats.allocated_pages() > 0 {
            stats.fragmented_bytes() as f64
                / (stats.allocated_pages() * stats.page_size() as u64) as f64
                * 100.0
        } else {
            0.0
        }
    );

    Ok(())
}

fn cmd_tables(path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let db = ReadOnlyDatabase::open(path)?;
    let txn = db.begin_read()?;

    let tables: Vec<_> = txn.list_tables()?.collect();
    let multimap_tables: Vec<_> = txn.list_multimap_tables()?.collect();

    if tables.is_empty() && multimap_tables.is_empty() {
        println!("No tables found.");
        return Ok(());
    }

    let mut pretty = PrettyTable::new();
    pretty.set_header(vec![
        Cell::new("Table").add_attribute(Attribute::Bold),
        Cell::new("Type").add_attribute(Attribute::Bold),
        Cell::new("Rows").add_attribute(Attribute::Bold),
        Cell::new("Stored").add_attribute(Attribute::Bold),
        Cell::new("Metadata").add_attribute(Attribute::Bold),
        Cell::new("Fragmented").add_attribute(Attribute::Bold),
        Cell::new("Depth").add_attribute(Attribute::Bold),
    ]);

    for handle in &tables {
        let untyped = txn.open_untyped_table(handle.clone())?;
        let len = untyped.len()?;
        let ts = untyped.stats()?;
        pretty.add_row(vec![
            Cell::new(handle.name()),
            Cell::new("table"),
            Cell::new(len),
            Cell::new(format_bytes(ts.stored_bytes())),
            Cell::new(format_bytes(ts.metadata_bytes())),
            Cell::new(format_bytes(ts.fragmented_bytes())),
            Cell::new(ts.tree_height()),
        ]);
    }

    for handle in &multimap_tables {
        let untyped = txn.open_untyped_multimap_table(handle.clone())?;
        let len = untyped.len()?;
        let ts = untyped.stats()?;
        pretty.add_row(vec![
            Cell::new(handle.name()),
            Cell::new("multimap"),
            Cell::new(len),
            Cell::new(format_bytes(ts.stored_bytes())),
            Cell::new(format_bytes(ts.metadata_bytes())),
            Cell::new(format_bytes(ts.fragmented_bytes())),
            Cell::new(ts.tree_height()),
        ]);
    }

    println!("{pretty}");
    Ok(())
}

fn cmd_stats(path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let file_size = std::fs::metadata(path)?.len();

    // Get DB-level stats from write transaction
    let db = Database::open(path)?;
    let txn = db.begin_write()?;
    let stats = txn.stats()?;
    txn.abort()?;

    // Get per-table stats from read transaction (has open_untyped_table)
    let rtxn = db.begin_read()?;
    let tables: Vec<_> = rtxn.list_tables()?.collect();
    let multimap_tables: Vec<_> = rtxn.list_multimap_tables()?.collect();

    let mut table_rows = Vec::new();
    for handle in &tables {
        let untyped = rtxn.open_untyped_table(handle.clone())?;
        let len = untyped.len()?;
        let ts = untyped.stats()?;
        table_rows.push((handle.name().to_string(), false, len, ts));
    }
    for handle in &multimap_tables {
        let untyped = rtxn.open_untyped_multimap_table(handle.clone())?;
        let len = untyped.len()?;
        let ts = untyped.stats()?;
        table_rows.push((handle.name().to_string(), true, len, ts));
    }

    drop(rtxn);
    drop(db);

    let total_allocated = stats.allocated_pages() * stats.page_size() as u64;
    let overhead = file_size.saturating_sub(total_allocated);

    println!("=== Database Statistics ===");
    println!();
    println!("File size:          {}", format_bytes(file_size));
    println!("Allocated pages:    {}", stats.allocated_pages());
    println!("Page size:          {} bytes", stats.page_size());
    println!("Total allocated:    {}", format_bytes(total_allocated));
    println!("Region overhead:    {}", format_bytes(overhead));
    println!();
    println!("--- Data Breakdown ---");
    println!("Stored data:        {}", format_bytes(stats.stored_bytes()));
    println!("Leaf pages:         {}", stats.leaf_pages());
    println!("Branch pages:       {}", stats.branch_pages());
    println!(
        "Metadata:           {}",
        format_bytes(stats.metadata_bytes())
    );
    println!(
        "Fragmented:         {}",
        format_bytes(stats.fragmented_bytes())
    );
    println!("Tree height:        {}", stats.tree_height());
    println!();
    println!("--- Space Efficiency ---");
    let used = stats.stored_bytes() + stats.metadata_bytes();
    println!(
        "Space efficiency:   {:.1}%",
        if file_size > 0 {
            used as f64 / file_size as f64 * 100.0
        } else {
            0.0
        }
    );
    println!(
        "Space amplification: {:.2}x",
        if used > 0 {
            file_size as f64 / used as f64
        } else {
            0.0
        }
    );

    if !table_rows.is_empty() {
        println!();
        println!("--- Per-Table ---");

        let mut pretty = PrettyTable::new();
        pretty.set_header(vec![
            Cell::new("Table").add_attribute(Attribute::Bold),
            Cell::new("Rows").add_attribute(Attribute::Bold),
            Cell::new("Leaf").add_attribute(Attribute::Bold),
            Cell::new("Branch").add_attribute(Attribute::Bold),
            Cell::new("Stored").add_attribute(Attribute::Bold),
            Cell::new("Meta").add_attribute(Attribute::Bold),
            Cell::new("Frag").add_attribute(Attribute::Bold),
            Cell::new("Depth").add_attribute(Attribute::Bold),
        ]);

        for (name, is_multimap, len, ts) in &table_rows {
            let display_name = if *is_multimap {
                format!("{name} (mm)")
            } else {
                name.clone()
            };
            pretty.add_row(vec![
                Cell::new(display_name),
                Cell::new(len),
                Cell::new(ts.leaf_pages()),
                Cell::new(ts.branch_pages()),
                Cell::new(format_bytes(ts.stored_bytes())),
                Cell::new(format_bytes(ts.metadata_bytes())),
                Cell::new(format_bytes(ts.fragmented_bytes())),
                Cell::new(ts.tree_height()),
            ]);
        }

        println!("{pretty}");
    }

    Ok(())
}

fn cmd_verify(path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    println!("Verifying database: {}", path.display());
    println!();

    let mut db = Database::open(path)?;
    match db.check_integrity() {
        Ok(true) => {
            println!("Integrity check PASSED");
            println!("All checksums valid, B-tree structure intact.");
        }
        Ok(false) => {
            println!("Integrity check FAILED — database was REPAIRED");
            println!("Some corruption was detected and automatically repaired.");
            println!("Review your data for completeness.");
        }
        Err(e) => {
            println!("Integrity check FAILED — UNRECOVERABLE");
            println!("Error: {e}");
            println!("The database file may be severely corrupted.");
            return Err(e.into());
        }
    }

    Ok(())
}

fn cmd_compact(path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let file_size_before = std::fs::metadata(path)?.len();
    println!(
        "Compacting database: {} ({})",
        path.display(),
        format_bytes(file_size_before)
    );

    let mut db = Database::open(path)?;
    let mut rounds = 0u32;
    loop {
        match db.compact() {
            Ok(true) => {
                rounds += 1;
            }
            Ok(false) => break,
            Err(e) => {
                eprintln!("Compaction error: {e}");
                return Err(e.into());
            }
        }
    }

    drop(db);
    let file_size_after = std::fs::metadata(path)?.len();

    if rounds == 0 {
        println!("No compaction needed — database is already compact.");
    } else {
        let saved = file_size_before.saturating_sub(file_size_after);
        println!(
            "Compacted in {} round{}: {} -> {} (saved {})",
            rounds,
            if rounds == 1 { "" } else { "s" },
            format_bytes(file_size_before),
            format_bytes(file_size_after),
            format_bytes(saved),
        );
    }

    Ok(())
}

fn cmd_dump(
    path: &PathBuf,
    table_name: &str,
    limit: usize,
    hex: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let db = ReadOnlyDatabase::open(path)?;
    let txn = db.begin_read()?;

    // Find the table handle by name
    let tables: Vec<_> = txn.list_tables()?.collect();
    let handle = tables
        .into_iter()
        .find(|h| h.name() == table_name)
        .ok_or_else(|| format!("table '{}' not found", table_name))?;

    let untyped = txn.open_untyped_table(handle)?;
    let len = untyped.len()?;
    let iter = untyped.iter_raw()?;

    println!("Table: {table_name}  ({len} rows, showing up to {limit})");
    println!();

    let mut pretty = PrettyTable::new();
    pretty.set_header(vec![
        Cell::new("#").add_attribute(Attribute::Bold),
        Cell::new("Key").add_attribute(Attribute::Bold),
        Cell::new("Value").add_attribute(Attribute::Bold),
    ]);

    let mut count = 0usize;
    for entry in iter {
        if count >= limit {
            break;
        }
        let entry = entry?;
        let key_display = format_raw_bytes(entry.key(), hex);
        let value_display = format_raw_bytes(entry.value(), hex);
        pretty.add_row(vec![
            Cell::new(count + 1),
            Cell::new(key_display),
            Cell::new(value_display),
        ]);
        count += 1;
    }

    if count == 0 {
        println!("(empty table)");
    } else {
        println!("{pretty}");
        if count >= limit && (len as usize) > limit {
            println!("... truncated ({} more rows)", len as usize - count);
        }
    }

    Ok(())
}

fn cmd_header(path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    // Read raw 320 bytes from the file
    const DB_HEADER_SIZE: usize = 320;
    const MAGIC: [u8; 9] = [b'r', b'e', b'd', b'b', 0x1A, 0x0A, 0xA9, 0x0D, 0x0A];

    let mut file = std::fs::File::open(path)?;
    let mut buf = [0u8; DB_HEADER_SIZE];
    file.read_exact(&mut buf)?;

    // Magic number (bytes 0-8)
    let magic_valid = buf[..9] == MAGIC;
    println!("=== File Header (320 bytes) ===");
    println!();
    println!(
        "Magic number:     {} {}",
        buf[..9]
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<Vec<_>>()
            .join(" "),
        if magic_valid { "(valid)" } else { "(INVALID)" },
    );

    // God byte (byte 9)
    let god = buf[9];
    let primary_slot = usize::from(god & 0x01 != 0);
    let recovery_required = god & 0x02 != 0;
    let two_phase_commit = god & 0x04 != 0;
    println!("God byte:         0x{god:02x}");
    println!("  Primary slot:   {primary_slot}");
    println!("  Recovery req:   {recovery_required}");
    println!("  2-phase commit: {two_phase_commit}");

    // Page size (bytes 12-15, after 2 bytes padding)
    let page_size = u32::from_le_bytes(buf[12..16].try_into().unwrap());
    println!("Page size:        {page_size} bytes");

    // Region layout (bytes 16-23)
    let region_header_pages = u32::from_le_bytes(buf[16..20].try_into().unwrap());
    let region_max_data_pages = u32::from_le_bytes(buf[20..24].try_into().unwrap());
    println!("Region header pg: {region_header_pages}");
    println!("Region max data:  {region_max_data_pages}");

    // Commit slots
    println!();
    for slot_idx in 0..2u8 {
        let offset = 64 + (slot_idx as usize) * 128;
        let slot = &buf[offset..offset + 128];
        let is_primary = slot_idx as usize == primary_slot;
        let label = if is_primary {
            " (PRIMARY)"
        } else {
            " (secondary)"
        };

        println!("--- Commit Slot {slot_idx}{label} ---");

        let version = slot[0];
        println!("  Format version: {version}");

        let user_root_present = slot[1] != 0;
        let system_root_present = slot[2] != 0;

        // BtreeHeader is at offset 8 within slot: PageNumber(8) + Checksum(16) + length(8) = 32 bytes
        if user_root_present {
            let root_page = u64::from_le_bytes(slot[8..16].try_into().unwrap());
            let root_checksum = u128::from_le_bytes(slot[16..32].try_into().unwrap());
            let root_length = u64::from_le_bytes(slot[32..40].try_into().unwrap());
            println!(
                "  User root:      page={root_page}, checksum=0x{root_checksum:032x}, length={root_length}"
            );
        } else {
            println!("  User root:      (null)");
        }

        if system_root_present {
            let root_page = u64::from_le_bytes(slot[40..48].try_into().unwrap());
            let root_checksum = u128::from_le_bytes(slot[48..64].try_into().unwrap());
            let root_length = u64::from_le_bytes(slot[64..72].try_into().unwrap());
            println!(
                "  System root:    page={root_page}, checksum=0x{root_checksum:032x}, length={root_length}"
            );
        } else {
            println!("  System root:    (null)");
        }

        // Slot layout: flags(8) + user_root(32) + system_root(32) + unused(32) + txid(8) + checksum(16) = 128
        let transaction_id = u64::from_le_bytes(slot[104..112].try_into().unwrap());
        println!("  Transaction ID: {transaction_id}");

        let stored_checksum = u128::from_le_bytes(slot[112..128].try_into().unwrap());
        let computed_checksum = xxhash_rust::xxh3::xxh3_128_with_seed(&slot[..112], 0);
        let checksum_valid = stored_checksum == computed_checksum;
        println!(
            "  Slot checksum:  0x{stored_checksum:032x} {}",
            if checksum_valid {
                "(valid)"
            } else {
                "(INVALID)"
            },
        );
    }

    // Compression algorithm (byte 36 in main header)
    // Only meaningful for format version 4+; earlier versions have garbage here
    let primary_version = {
        let primary_offset = if primary_slot == 0 { 64 } else { 64 + 128 };
        buf[primary_offset]
    };
    let compression_algo = buf[36];
    let compression_display = if primary_version >= 4 {
        match compression_algo {
            0 => "none".to_string(),
            1 => "lz4".to_string(),
            2 => "zstd".to_string(),
            other => format!("unknown ({other})"),
        }
    } else {
        "none (v3 format)".to_string()
    };
    println!();
    println!("Compression algo: {compression_display} (byte 36 = 0x{compression_algo:02x})");

    // Full regions and trailing (bytes 24-31 in main header)
    let full_regions = u32::from_le_bytes(buf[24..28].try_into().unwrap());
    let trailing_pages = u32::from_le_bytes(buf[28..32].try_into().unwrap());
    println!();
    println!("Full regions:     {full_regions}");
    println!("Trailing pages:   {trailing_pages}");

    // File size for context
    let file_size = std::fs::metadata(path)?.len();
    println!("File size:        {}", format_bytes(file_size));

    Ok(())
}

/// Format raw bytes for display. Attempts UTF-8 decoding; falls back to hex.
fn format_raw_bytes(data: &[u8], force_hex: bool) -> String {
    if force_hex {
        return format_hex(data);
    }

    if let Ok(s) = std::str::from_utf8(data)
        && s.chars().all(|c| !c.is_control() || c == '\n' || c == '\t')
    {
        if s.len() > 120 {
            return format!("{}...", &s[..120]);
        }
        return s.to_string();
    }

    // Fall back to hex with length prefix
    format_hex(data)
}

fn format_hex(data: &[u8]) -> String {
    if data.len() <= 32 {
        data.iter()
            .map(|b| format!("{b:02x}"))
            .collect::<Vec<_>>()
            .join(" ")
    } else {
        let preview: String = data[..32]
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<Vec<_>>()
            .join(" ");
        format!("{preview}... ({} bytes)", data.len())
    }
}

/// Read the compression algorithm name from the raw database header.
/// Returns a human-readable string like "lz4", "zstd", or "none".
fn read_compression_name(path: &PathBuf) -> String {
    let Ok(mut file) = std::fs::File::open(path) else {
        return "unknown".to_string();
    };
    let mut buf = [0u8; 320];
    if file.read_exact(&mut buf).is_err() {
        return "unknown".to_string();
    }
    // Primary slot version determines if compression byte is valid
    let god = buf[9];
    let primary_slot_offset = if god & 0x01 != 0 { 64 + 128 } else { 64 };
    let version = buf[primary_slot_offset];
    if version < 4 {
        return "none".to_string();
    }
    match buf[36] {
        0 => "none".to_string(),
        1 => "lz4".to_string(),
        2 => "zstd".to_string(),
        other => format!("unknown ({other})"),
    }
}

fn format_bytes(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * KIB;
    const GIB: u64 = 1024 * MIB;

    if bytes >= GIB {
        format!("{:.2} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.2} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.2} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{bytes} B")
    }
}
