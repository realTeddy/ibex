use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use eframe::egui;
use eframe::egui::text::LayoutJob;
use mongodb::bson::{Bson, Document};
use mongodb::Client;
use mongodb::options::ClientOptions;
use serde_json::Value as JsonValue;
use tokio::runtime::Handle;

// ─── Compile-time constants ──────────────────────────────────────────────────
/// Line height for the monospace font in the results panel (px).
const RESULT_LINE_HEIGHT: f32 = 16.0;
/// Extra lines to render above/below the visible region for smooth scrolling.
const OVERSCAN_LINES: usize = 20;

// ─── Async MongoDB backend ───────────────────────────────────────────────────

struct MongoConnection {
    client: Client,
    current_db: String,
}

unsafe impl Send for MongoConnection {}

struct QueryResult {
    /// Pre-split lines of the result output (avoids giant intermediate String).
    lines: Vec<String>,
    elapsed: Duration,
    _is_error: bool,
}

/// Optional modifiers that can be chained after a primary query method.
/// e.g. `db.coll.find({}).limit(100).skip(10).sort({name: 1})`
#[derive(Default, Clone)]
struct QueryModifiers {
    limit: Option<i64>,
    skip: Option<u64>,
    sort: Option<String>,
    projection: Option<String>,
}

enum MongoOp {
    UseDb(String),
    Find {
        collection: String,
        filter: Option<String>,
        modifiers: QueryModifiers,
    },
    CountDocuments {
        collection: String,
        filter: Option<String>,
    },
    InsertOne {
        collection: String,
        doc: String,
    },
    DeleteOne {
        collection: String,
        filter: String,
    },
    DeleteMany {
        collection: String,
        filter: String,
    },
    Aggregate {
        collection: String,
        pipeline: String,
        modifiers: QueryModifiers,
    },
    ShowDbs,
    ShowCollections,
}

// ─── Script parser ───────────────────────────────────────────────────────────

fn parse_script(script: &str) -> Result<Vec<MongoOp>, String> {
    let mut ops = Vec::new();
    let statements = split_statements(script);

    for stmt in &statements {
        let stmt = stmt.trim();
        if stmt.is_empty() {
            continue;
        }

        if let Some(db_name) = stmt.strip_prefix("use ").or_else(|| stmt.strip_prefix("use\t")) {
            let db_name = db_name.trim().trim_end_matches(';');
            if db_name.is_empty() {
                return Err("`use` requires a database name".into());
            }
            ops.push(MongoOp::UseDb(db_name.to_string()));
        } else if stmt == "show dbs" || stmt == "show databases" {
            ops.push(MongoOp::ShowDbs);
        } else if stmt == "show collections" || stmt == "show tables" {
            ops.push(MongoOp::ShowCollections);
        } else if stmt.starts_with("db.") {
            ops.push(parse_db_command(stmt)?);
        } else {
            return Err(format!("Unknown command: {stmt}"));
        }
    }
    Ok(ops)
}

fn split_statements(script: &str) -> Vec<String> {
    let mut stmts = Vec::new();
    let mut current = String::new();
    let mut depth: i32 = 0;
    let mut in_string = false;
    let mut escape_next = false;
    let mut string_char: char = '"';

    for ch in script.chars() {
        if escape_next {
            current.push(ch);
            escape_next = false;
            continue;
        }
        if ch == '\\' && in_string {
            current.push(ch);
            escape_next = true;
            continue;
        }
        if in_string {
            current.push(ch);
            if ch == string_char {
                in_string = false;
            }
            continue;
        }
        match ch {
            '"' | '\'' => {
                in_string = true;
                string_char = ch;
                current.push(ch);
            }
            '{' | '[' | '(' => {
                depth += 1;
                current.push(ch);
            }
            '}' | ']' | ')' => {
                depth -= 1;
                current.push(ch);
            }
            ';' | '\n' if depth == 0 => {
                let trimmed = current.trim().to_string();
                if !trimmed.is_empty() {
                    stmts.push(trimmed);
                }
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    let trimmed = current.trim().to_string();
    if !trimmed.is_empty() {
        stmts.push(trimmed);
    }
    stmts
}

fn parse_db_command(stmt: &str) -> Result<MongoOp, String> {
    let after_db = &stmt[3..]; // skip "db."
    let dot_pos = after_db
        .find('.')
        .ok_or("Expected db.<collection>.<method>(...)")?;
    let collection = after_db[..dot_pos].to_string();
    let rest = &after_db[dot_pos + 1..];

    // Parse chained method calls: method(args).method2(args2).method3(args3)...
    let calls = parse_chained_calls(rest)?;
    if calls.is_empty() {
        return Err("Expected method call with parentheses".into());
    }

    let (method, args) = &calls[0];
    let args = args.trim();

    // Parse any trailing modifier calls (.limit, .skip, .sort, .projection)
    let modifiers = parse_modifiers(&calls[1..])?;

    match method.as_str() {
        "find" => Ok(MongoOp::Find {
            collection,
            filter: if args.is_empty() {
                None
            } else {
                Some(args.to_string())
            },
            modifiers,
        }),
        "countDocuments" | "count" => Ok(MongoOp::CountDocuments {
            collection,
            filter: if args.is_empty() {
                None
            } else {
                Some(args.to_string())
            },
        }),
        "insertOne" => {
            if args.is_empty() {
                return Err("insertOne requires a document argument".into());
            }
            Ok(MongoOp::InsertOne {
                collection,
                doc: args.to_string(),
            })
        }
        "deleteOne" => {
            if args.is_empty() {
                return Err("deleteOne requires a filter argument".into());
            }
            Ok(MongoOp::DeleteOne {
                collection,
                filter: args.to_string(),
            })
        }
        "deleteMany" => {
            if args.is_empty() {
                return Err("deleteMany requires a filter argument".into());
            }
            Ok(MongoOp::DeleteMany {
                collection,
                filter: args.to_string(),
            })
        }
        "aggregate" => {
            if args.is_empty() {
                return Err("aggregate requires a pipeline argument".into());
            }
            Ok(MongoOp::Aggregate {
                collection,
                pipeline: args.to_string(),
                modifiers,
            })
        }
        _ => Err(format!("Unsupported method: {method}")),
    }
}

/// Parse a chain of method calls like `find({}).limit(100).sort({a:1})`
/// Returns a Vec of (method_name, args_string) tuples.
fn parse_chained_calls(input: &str) -> Result<Vec<(String, String)>, String> {
    let mut calls = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let len = chars.len();
    let mut pos = 0;

    loop {
        // Skip leading dot (for second+ calls)
        if pos > 0 {
            if pos >= len || chars[pos] != '.' {
                break;
            }
            pos += 1; // skip '.'
        }

        // Read method name
        let name_start = pos;
        while pos < len && (chars[pos].is_alphanumeric() || chars[pos] == '_') {
            pos += 1;
        }
        if pos == name_start {
            break;
        }
        let method_name: String = chars[name_start..pos].iter().collect();

        // Expect '('
        if pos >= len || chars[pos] != '(' {
            return Err(format!("Expected '(' after method '{method_name}'"));
        }
        pos += 1; // skip '('

        // Find matching ')' respecting nesting and strings
        let args_start = pos;
        let mut depth: i32 = 1;
        let mut in_string = false;
        let mut string_char = '"';

        while pos < len && depth > 0 {
            let ch = chars[pos];
            if in_string {
                if ch == '\\' && pos + 1 < len {
                    pos += 1; // skip escaped char
                } else if ch == string_char {
                    in_string = false;
                }
            } else {
                match ch {
                    '"' | '\'' => {
                        in_string = true;
                        string_char = ch;
                    }
                    '(' | '{' | '[' => depth += 1,
                    ')' | '}' | ']' => depth -= 1,
                    _ => {}
                }
            }
            if depth > 0 {
                pos += 1;
            }
        }

        if depth != 0 {
            return Err(format!("Unmatched parenthesis in '{method_name}(...)' call"));
        }

        let args: String = chars[args_start..pos].iter().collect();
        pos += 1; // skip closing ')'

        calls.push((method_name, args));
    }

    Ok(calls)
}

/// Parse modifier calls (limit, skip, sort, projection) from chained calls.
fn parse_modifiers(calls: &[(String, String)]) -> Result<QueryModifiers, String> {
    let mut mods = QueryModifiers::default();
    for (method, args) in calls {
        let args = args.trim();
        match method.as_str() {
            "limit" => {
                mods.limit = Some(
                    args.parse::<i64>()
                        .map_err(|_| format!("Invalid limit value: '{args}'"))?,
                );
            }
            "skip" => {
                mods.skip = Some(
                    args.parse::<u64>()
                        .map_err(|_| format!("Invalid skip value: '{args}'"))?,
                );
            }
            "sort" => {
                if args.is_empty() {
                    return Err("sort() requires a document argument".into());
                }
                mods.sort = Some(args.to_string());
            }
            "projection" | "project" => {
                if args.is_empty() {
                    return Err("projection() requires a document argument".into());
                }
                mods.projection = Some(args.to_string());
            }
            other => {
                return Err(format!("Unsupported chained method: .{other}()"));
            }
        }
    }
    Ok(mods)
}

// ─── Relaxed JSON → BSON helpers ─────────────────────────────────────────────

fn relaxed_json_to_doc(input: &str) -> Result<Document, String> {
    if let Ok(val) = serde_json::from_str::<JsonValue>(input) {
        return json_value_to_doc(&val);
    }
    let quoted = add_quotes_to_keys(input);
    let val: JsonValue =
        serde_json::from_str(&quoted).map_err(|e| format!("Invalid filter/document: {e}"))?;
    json_value_to_doc(&val)
}

fn add_quotes_to_keys(input: &str) -> String {
    let mut result = String::with_capacity(input.len() + 32);
    let chars: Vec<char> = input.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        let ch = chars[i];
        if ch == '"' || ch == '\'' {
            let quote = ch;
            result.push('"');
            i += 1;
            while i < len && chars[i] != quote {
                if chars[i] == '\\' && i + 1 < len {
                    result.push(chars[i]);
                    result.push(chars[i + 1]);
                    i += 2;
                } else {
                    result.push(chars[i]);
                    i += 1;
                }
            }
            result.push('"');
            if i < len {
                i += 1;
            }
        } else if ch.is_alphabetic() || ch == '_' || ch == '$' {
            let start = i;
            while i < len
                && (chars[i].is_alphanumeric()
                    || chars[i] == '_'
                    || chars[i] == '$'
                    || chars[i] == '.')
            {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            let mut j = i;
            while j < len && chars[j].is_whitespace() {
                j += 1;
            }
            if j < len && chars[j] == ':' {
                result.push('"');
                result.push_str(&word);
                result.push('"');
            } else {
                match word.as_str() {
                    "true" | "false" | "null" => result.push_str(&word),
                    _ => {
                        result.push('"');
                        result.push_str(&word);
                        result.push('"');
                    }
                }
            }
        } else {
            result.push(ch);
            i += 1;
        }
    }
    result
}

fn json_value_to_doc(val: &JsonValue) -> Result<Document, String> {
    match val {
        JsonValue::Object(map) => {
            let mut doc = Document::new();
            for (k, v) in map {
                doc.insert(k.clone(), json_value_to_bson(v));
            }
            Ok(doc)
        }
        _ => Err("Expected a JSON object".into()),
    }
}

fn json_value_to_bson(val: &JsonValue) -> Bson {
    match val {
        JsonValue::Null => Bson::Null,
        JsonValue::Bool(b) => Bson::Boolean(*b),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Bson::Int64(i)
            } else if let Some(f) = n.as_f64() {
                Bson::Double(f)
            } else {
                Bson::Null
            }
        }
        JsonValue::String(s) => Bson::String(s.clone()),
        JsonValue::Array(arr) => Bson::Array(arr.iter().map(json_value_to_bson).collect()),
        JsonValue::Object(map) => {
            let mut doc = Document::new();
            for (k, v) in map {
                doc.insert(k.clone(), json_value_to_bson(v));
            }
            Bson::Document(doc)
        }
    }
}

fn json_value_to_bson_array(val: &JsonValue) -> Result<Vec<Document>, String> {
    match val {
        JsonValue::Array(arr) => {
            let mut docs = Vec::new();
            for item in arr {
                docs.push(json_value_to_doc(item)?);
            }
            Ok(docs)
        }
        _ => Err("Expected a JSON array for pipeline".into()),
    }
}

// ─── Query executor ──────────────────────────────────────────────────────────

async fn execute_ops(conn: &mut MongoConnection, ops: Vec<MongoOp>) -> Vec<QueryResult> {
    use futures_util::TryStreamExt;

    let mut results = Vec::new();

    for op in ops {
        let start = Instant::now();
        let result = match op {
            MongoOp::UseDb(name) => {
                conn.current_db = name.clone();
                QueryResult {
                    lines: vec![format!("switched to db: {name}")],
                    elapsed: start.elapsed(),
                    _is_error: false,
                }
            }
            MongoOp::ShowDbs => match conn.client.list_database_names().await {
                Ok(names) => QueryResult {
                    lines: names,
                    elapsed: start.elapsed(),
                    _is_error: false,
                },
                Err(e) => QueryResult {
                    lines: vec![format!("Error: {e}")],
                    elapsed: start.elapsed(),
                    _is_error: true,
                },
            },
            MongoOp::ShowCollections => {
                if conn.current_db.is_empty() {
                    QueryResult {
                        lines: vec!["Error: no database selected. Use `use <db>` first.".into()],
                        elapsed: start.elapsed(),
                        _is_error: true,
                    }
                } else {
                    let db = conn.client.database(&conn.current_db);
                    match db.list_collection_names().await {
                        Ok(names) => QueryResult {
                            lines: if names.is_empty() {
                                vec!["(no collections)".into()]
                            } else {
                                names
                            },
                            elapsed: start.elapsed(),
                            _is_error: false,
                        },
                        Err(e) => QueryResult {
                            lines: vec![format!("Error: {e}")],
                            elapsed: start.elapsed(),
                            _is_error: true,
                        },
                    }
                }
            }
            MongoOp::Find { collection, filter, modifiers } => {
                if conn.current_db.is_empty() {
                    QueryResult {
                        lines: vec!["Error: no database selected. Use `use <db>` first.".into()],
                        elapsed: start.elapsed(),
                        _is_error: true,
                    }
                } else {
                    let db = conn.client.database(&conn.current_db);
                    let coll = db.collection::<Document>(&collection);
                    let filter_doc = match &filter {
                        Some(f) => match relaxed_json_to_doc(f) {
                            Ok(d) => d,
                            Err(e) => {
                                results.push(QueryResult {
                                    lines: vec![format!("Error parsing filter: {e}")],
                                    elapsed: start.elapsed(),
                                    _is_error: true,
                                });
                                continue;
                            }
                        },
                        None => Document::new(),
                    };
                    // Build find with chained modifiers
                    let mut find = coll.find(filter_doc);
                    let effective_limit = modifiers.limit.unwrap_or(100);
                    find = find.limit(effective_limit);
                    if let Some(skip) = modifiers.skip {
                        find = find.skip(skip);
                    }
                    if let Some(ref sort_str) = modifiers.sort {
                        match relaxed_json_to_doc(sort_str) {
                            Ok(sort_doc) => { find = find.sort(sort_doc); }
                            Err(e) => {
                                results.push(QueryResult {
                                    lines: vec![format!("Error parsing sort: {e}")],
                                    elapsed: start.elapsed(),
                                    _is_error: true,
                                });
                                continue;
                            }
                        }
                    }
                    if let Some(ref proj_str) = modifiers.projection {
                        match relaxed_json_to_doc(proj_str) {
                            Ok(proj_doc) => { find = find.projection(proj_doc); }
                            Err(e) => {
                                results.push(QueryResult {
                                    lines: vec![format!("Error parsing projection: {e}")],
                                    elapsed: start.elapsed(),
                                    _is_error: true,
                                });
                                continue;
                            }
                        }
                    }
                    match find.await {
                        Ok(mut cursor) => {
                            let max = effective_limit.unsigned_abs() as usize;
                            let mut docs = Vec::new();
                            while let Ok(Some(doc)) = cursor.try_next().await {
                                docs.push(doc);
                                if docs.len() >= max {
                                    break;
                                }
                            }
                            let doc_count = docs.len();
                            let mut lines = Vec::new();
                            format_documents_to_lines(&docs, &mut lines);
                            // Drop docs immediately to free BSON memory before
                            // we finish building the result
                            drop(docs);
                            lines.push(String::new());
                            lines.push(format!("({doc_count} document(s))"));
                            QueryResult {
                                lines,
                                elapsed: start.elapsed(),
                                _is_error: false,
                            }
                        }
                        Err(e) => QueryResult {
                            lines: vec![format!("Error: {e}")],
                            elapsed: start.elapsed(),
                            _is_error: true,
                        },
                    }
                }
            }
            MongoOp::CountDocuments { collection, filter } => {
                if conn.current_db.is_empty() {
                    QueryResult {
                        lines: vec!["Error: no database selected. Use `use <db>` first.".into()],
                        elapsed: start.elapsed(),
                        _is_error: true,
                    }
                } else {
                    let db = conn.client.database(&conn.current_db);
                    let coll = db.collection::<Document>(&collection);
                    let filter_doc = match &filter {
                        Some(f) => match relaxed_json_to_doc(f) {
                            Ok(d) => d,
                            Err(e) => {
                                results.push(QueryResult {
                                    lines: vec![format!("Error parsing filter: {e}")],
                                    elapsed: start.elapsed(),
                                    _is_error: true,
                                });
                                continue;
                            }
                        },
                        None => Document::new(),
                    };
                    match coll.count_documents(filter_doc).await {
                        Ok(count) => QueryResult {
                            lines: vec![format!("{count}")],
                            elapsed: start.elapsed(),
                            _is_error: false,
                        },
                        Err(e) => QueryResult {
                            lines: vec![format!("Error: {e}")],
                            elapsed: start.elapsed(),
                            _is_error: true,
                        },
                    }
                }
            }
            MongoOp::InsertOne { collection, doc } => {
                if conn.current_db.is_empty() {
                    QueryResult {
                        lines: vec!["Error: no database selected. Use `use <db>` first.".into()],
                        elapsed: start.elapsed(),
                        _is_error: true,
                    }
                } else {
                    let db = conn.client.database(&conn.current_db);
                    let coll = db.collection::<Document>(&collection);
                    match relaxed_json_to_doc(&doc) {
                        Ok(document) => match coll.insert_one(document).await {
                            Ok(r) => QueryResult {
                                lines: vec![format!("Inserted document with _id: {}", r.inserted_id)],
                                elapsed: start.elapsed(),
                                _is_error: false,
                            },
                            Err(e) => QueryResult {
                                lines: vec![format!("Error: {e}")],
                                elapsed: start.elapsed(),
                                _is_error: true,
                            },
                        },
                        Err(e) => QueryResult {
                            lines: vec![format!("Error parsing document: {e}")],
                            elapsed: start.elapsed(),
                            _is_error: true,
                        },
                    }
                }
            }
            MongoOp::DeleteOne { collection, filter } => {
                if conn.current_db.is_empty() {
                    QueryResult {
                        lines: vec!["Error: no database selected. Use `use <db>` first.".into()],
                        elapsed: start.elapsed(),
                        _is_error: true,
                    }
                } else {
                    let db = conn.client.database(&conn.current_db);
                    let coll = db.collection::<Document>(&collection);
                    match relaxed_json_to_doc(&filter) {
                        Ok(f) => match coll.delete_one(f).await {
                            Ok(r) => QueryResult {
                                lines: vec![format!("Deleted {} document(s)", r.deleted_count)],
                                elapsed: start.elapsed(),
                                _is_error: false,
                            },
                            Err(e) => QueryResult {
                                lines: vec![format!("Error: {e}")],
                                elapsed: start.elapsed(),
                                _is_error: true,
                            },
                        },
                        Err(e) => QueryResult {
                            lines: vec![format!("Error parsing filter: {e}")],
                            elapsed: start.elapsed(),
                            _is_error: true,
                        },
                    }
                }
            }
            MongoOp::DeleteMany { collection, filter } => {
                if conn.current_db.is_empty() {
                    QueryResult {
                        lines: vec!["Error: no database selected. Use `use <db>` first.".into()],
                        elapsed: start.elapsed(),
                        _is_error: true,
                    }
                } else {
                    let db = conn.client.database(&conn.current_db);
                    let coll = db.collection::<Document>(&collection);
                    match relaxed_json_to_doc(&filter) {
                        Ok(f) => match coll.delete_many(f).await {
                            Ok(r) => QueryResult {
                                lines: vec![format!("Deleted {} document(s)", r.deleted_count)],
                                elapsed: start.elapsed(),
                                _is_error: false,
                            },
                            Err(e) => QueryResult {
                                lines: vec![format!("Error: {e}")],
                                elapsed: start.elapsed(),
                                _is_error: true,
                            },
                        },
                        Err(e) => QueryResult {
                            lines: vec![format!("Error parsing filter: {e}")],
                            elapsed: start.elapsed(),
                            _is_error: true,
                        },
                    }
                }
            }
            MongoOp::Aggregate {
                collection,
                pipeline,
                modifiers,
            } => {
                if conn.current_db.is_empty() {
                    QueryResult {
                        lines: vec!["Error: no database selected. Use `use <db>` first.".into()],
                        elapsed: start.elapsed(),
                        _is_error: true,
                    }
                } else {
                    let db = conn.client.database(&conn.current_db);
                    let coll = db.collection::<Document>(&collection);
                    let pipeline_str = pipeline.trim();
                    let pipeline_val: Result<JsonValue, _> =
                        serde_json::from_str(pipeline_str).or_else(|_| {
                            let quoted = add_quotes_to_keys(pipeline_str);
                            serde_json::from_str(&quoted)
                        });
                    let max_docs = modifiers.limit.map(|l| l.unsigned_abs() as usize).unwrap_or(100);
                    match pipeline_val {
                        Ok(val) => match json_value_to_bson_array(&val) {
                            Ok(stages) => match coll.aggregate(stages).await {
                                Ok(mut cursor) => {
                                    let mut docs = Vec::new();
                                    while let Ok(Some(doc)) = cursor.try_next().await {
                                        docs.push(doc);
                                        if docs.len() >= max_docs {
                                            break;
                                        }
                                    }
                                    let doc_count = docs.len();
                                    let mut lines = Vec::new();
                                    format_documents_to_lines(&docs, &mut lines);
                                    drop(docs);
                                    lines.push(String::new());
                                    lines.push(format!("({doc_count} document(s))"));
                                    QueryResult {
                                        lines,
                                        elapsed: start.elapsed(),
                                        _is_error: false,
                                    }
                                }
                                Err(e) => QueryResult {
                                    lines: vec![format!("Error: {e}")],
                                    elapsed: start.elapsed(),
                                    _is_error: true,
                                },
                            },
                            Err(e) => QueryResult {
                                lines: vec![format!("Error parsing pipeline: {e}")],
                                elapsed: start.elapsed(),
                                _is_error: true,
                            },
                        },
                        Err(e) => QueryResult {
                            lines: vec![format!("Error parsing pipeline JSON: {e}")],
                            elapsed: start.elapsed(),
                            _is_error: true,
                        },
                    }
                }
            }
        };
        results.push(result);
    }
    results
}

/// Pretty-print BSON documents as JSON lines. Each line is a separate String
/// to feed directly into the virtualized renderer without building a giant
/// intermediate buffer. Uses a line-splitting writer to avoid any monolithic
/// String allocation.
fn format_documents_to_lines(docs: &[Document], out: &mut Vec<String>) {
    if docs.is_empty() {
        out.push("(no results)".to_string());
        return;
    }
    // Pre-allocate a byte buffer we reuse per-document to avoid repeated allocs.
    // serde_json writes into this, then we split it into lines.
    let mut buf: Vec<u8> = Vec::with_capacity(4096);
    for (i, doc) in docs.iter().enumerate() {
        if i > 0 {
            out.push("---".to_string());
        }
        buf.clear();
        if serde_json::to_writer_pretty(&mut buf, doc).is_err() {
            out.push(format!("{doc:?}"));
            continue;
        }
        // SAFETY: serde_json always writes valid UTF-8.
        let text = unsafe { std::str::from_utf8_unchecked(&buf) };
        for line in text.lines() {
            out.push(line.to_string());
        }
    }
}

// ─── Syntax highlighting (zero-allocation, slice-based) ──────────────────────

// Theme colors (dark mode)
const COL_KEYWORD: egui::Color32 = egui::Color32::from_rgb(198, 120, 221); // purple
const COL_METHOD: egui::Color32 = egui::Color32::from_rgb(97, 175, 239); // blue
const COL_STRING: egui::Color32 = egui::Color32::from_rgb(152, 195, 121); // green
const COL_NUMBER: egui::Color32 = egui::Color32::from_rgb(209, 154, 102); // orange
const COL_BRACE: egui::Color32 = egui::Color32::from_rgb(224, 108, 117); // red
const COL_BOOL: egui::Color32 = egui::Color32::from_rgb(209, 154, 102); // orange
const COL_NULL: egui::Color32 = egui::Color32::from_rgb(224, 108, 117); // red
const COL_KEY: egui::Color32 = egui::Color32::from_rgb(229, 192, 123); // yellow
const COL_SEPARATOR: egui::Color32 = egui::Color32::from_rgb(92, 99, 112); // gray
const COL_DEFAULT: egui::Color32 = egui::Color32::from_rgb(171, 178, 191); // light gray
const COL_DB: egui::Color32 = egui::Color32::from_rgb(86, 182, 194); // cyan

const MONGO_KEYWORDS: &[&str] = &[
    "use",
    "show",
    "dbs",
    "databases",
    "collections",
    "tables",
];
const MONGO_METHODS: &[&str] = &[
    "find",
    "countDocuments",
    "count",
    "insertOne",
    "deleteOne",
    "deleteMany",
    "aggregate",
    "updateOne",
    "updateMany",
    "limit",
    "skip",
    "sort",
    "projection",
    "project",
];

/// Append a slice with a given color, reusing the TextFormat allocation when
/// the color matches the previous append.
#[inline(always)]
fn push_span(job: &mut LayoutJob, text: &str, color: egui::Color32, font_id: &egui::FontId) {
    job.append(
        text,
        0.0,
        egui::TextFormat {
            font_id: font_id.clone(),
            color,
            ..Default::default()
        },
    );
}

/// Check if byte is start of an ASCII identifier character.
#[inline(always)]
fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_' || b == b'$'
}

#[inline(always)]
fn is_ident_cont(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'$'
}

/// Syntax-highlight a mongo shell script for the editor.
/// Operates directly on `&str` byte slices -- zero Vec<char> allocation.
fn highlight_script(text: &str, font_id: &egui::FontId) -> LayoutJob {
    let mut job = LayoutJob::default();
    let bytes = text.as_bytes();
    let len = bytes.len();
    let mut i = 0;

    while i < len {
        let b = bytes[i];

        // ── Strings ──
        if b == b'"' || b == b'\'' {
            let quote = b;
            let start = i;
            i += 1;
            while i < len && bytes[i] != quote {
                if bytes[i] == b'\\' && i + 1 < len {
                    i += 1;
                }
                i += 1;
            }
            if i < len {
                i += 1;
            }
            push_span(&mut job, &text[start..i], COL_STRING, font_id);
            continue;
        }

        // ── "db" prefix ──
        if b == b'd' && i + 1 < len && bytes[i + 1] == b'b' && (i + 2 >= len || bytes[i + 2] == b'.') {
            push_span(&mut job, &text[i..i + 2], COL_DB, font_id);
            i += 2;
            continue;
        }

        // ── Words (keywords, methods, booleans, identifiers) ──
        if is_ident_start(b) {
            let start = i;
            i += 1;
            while i < len && is_ident_cont(bytes[i]) {
                i += 1;
            }
            let word = &text[start..i];
            let color = if MONGO_KEYWORDS.contains(&word) {
                COL_KEYWORD
            } else if MONGO_METHODS.contains(&word) {
                COL_METHOD
            } else if word == "true" || word == "false" {
                COL_BOOL
            } else if word == "null" {
                COL_NULL
            } else {
                COL_DEFAULT
            };
            push_span(&mut job, word, color, font_id);
            continue;
        }

        // ── Numbers ──
        if b.is_ascii_digit() || (b == b'-' && i + 1 < len && bytes[i + 1].is_ascii_digit()) {
            let start = i;
            if b == b'-' {
                i += 1;
            }
            while i < len && (bytes[i].is_ascii_digit() || bytes[i] == b'.') {
                i += 1;
            }
            push_span(&mut job, &text[start..i], COL_NUMBER, font_id);
            continue;
        }

        // ── Braces / brackets / parens ──
        if matches!(b, b'{' | b'}' | b'[' | b']' | b'(' | b')') {
            push_span(&mut job, &text[i..i + 1], COL_BRACE, font_id);
            i += 1;
            continue;
        }

        // ── Batch consecutive "default" bytes (whitespace, dots, colons, etc.) ──
        {
            let start = i;
            i += 1;
            while i < len {
                let c = bytes[i];
                if c == b'"' || c == b'\'' || is_ident_start(c) || c.is_ascii_digit()
                    || matches!(c, b'{' | b'}' | b'[' | b']' | b'(' | b')')
                    || (c == b'-' && i + 1 < len && bytes[i + 1].is_ascii_digit())
                {
                    break;
                }
                i += 1;
            }
            push_span(&mut job, &text[start..i], COL_DEFAULT, font_id);
        }
    }

    job
}

/// Syntax-highlight a single line of JSON result text.
/// Operates on byte slices -- no allocations beyond the LayoutJob sections.
fn highlight_json_line(text: &str, font_id: &egui::FontId, job: &mut LayoutJob) {
    let bytes = text.as_bytes();
    let len = bytes.len();
    let mut i = 0;

    while i < len {
        let b = bytes[i];

        // ── Separator lines "---" ──
        if b == b'-' && i + 2 < len && bytes[i + 1] == b'-' && bytes[i + 2] == b'-' {
            let start = i;
            while i < len && bytes[i] == b'-' {
                i += 1;
            }
            push_span(job, &text[start..i], COL_SEPARATOR, font_id);
            continue;
        }

        // ── Strings -- detect if JSON key (followed by ':') ──
        if b == b'"' {
            let start = i;
            i += 1;
            while i < len && bytes[i] != b'"' {
                if bytes[i] == b'\\' && i + 1 < len {
                    i += 1;
                }
                i += 1;
            }
            if i < len {
                i += 1;
            }
            // Look ahead past whitespace for ':'
            let mut j = i;
            while j < len && bytes[j] == b' ' {
                j += 1;
            }
            let color = if j < len && bytes[j] == b':' { COL_KEY } else { COL_STRING };
            push_span(job, &text[start..i], color, font_id);
            continue;
        }

        // ── Words: true, false, null, or other alphabetic ──
        if b.is_ascii_alphabetic() {
            let start = i;
            i += 1;
            while i < len && bytes[i].is_ascii_alphanumeric() {
                i += 1;
            }
            let word = &text[start..i];
            let color = match word {
                "true" | "false" => COL_BOOL,
                "null" => COL_NULL,
                _ => COL_DEFAULT,
            };
            push_span(job, word, color, font_id);
            continue;
        }

        // ── Numbers ──
        if b.is_ascii_digit() || (b == b'-' && i + 1 < len && bytes[i + 1].is_ascii_digit()) {
            let start = i;
            if b == b'-' {
                i += 1;
            }
            while i < len {
                let c = bytes[i];
                if c.is_ascii_digit() || c == b'.' || c == b'e' || c == b'E' {
                    i += 1;
                } else if (c == b'+' || c == b'-') && i > start && (bytes[i - 1] == b'e' || bytes[i - 1] == b'E') {
                    i += 1;
                } else {
                    break;
                }
            }
            push_span(job, &text[start..i], COL_NUMBER, font_id);
            continue;
        }

        // ── Braces / brackets ──
        if matches!(b, b'{' | b'}' | b'[' | b']') {
            push_span(job, &text[i..i + 1], COL_BRACE, font_id);
            i += 1;
            continue;
        }

        // ── Batch remaining default-colored bytes ──
        {
            let start = i;
            i += 1;
            while i < len {
                let c = bytes[i];
                if c == b'"' || c == b'-' || c.is_ascii_alphanumeric()
                    || matches!(c, b'{' | b'}' | b'[' | b']')
                {
                    break;
                }
                i += 1;
            }
            push_span(job, &text[start..i], COL_DEFAULT, font_id);
        }
    }
}

// ─── GUI Application ─────────────────────────────────────────────────────────

struct SharedState {
    /// Result lines behind an Arc -- shared with the UI cache (no cloning).
    result_lines: Arc<Vec<String>>,
    is_running: bool,
    status_message: String,
    connected: bool,
    /// Monotonic generation counter -- bumped every time results or status changes.
    generation: u64,
}

struct IbexApp {
    connection_string: String,
    script: String,
    shared: Arc<Mutex<SharedState>>,
    connection: Arc<tokio::sync::Mutex<Option<MongoConnection>>>,
    rt_handle: Handle,
    /// Keep the runtime thread alive for the lifetime of the app
    _rt_thread: std::thread::JoinHandle<()>,
    // ── Cached copies ── only updated when generation changes
    result_lines_cache: Arc<Vec<String>>,
    status_cache: String,
    is_running_cache: bool,
    connected_cache: bool,
    last_generation: u64,
    // ── Script highlight cache ──
    script_highlight_cache: Option<(u64, LayoutJob)>,
    script_hash: u64,
}

impl Default for IbexApp {
    fn default() -> Self {
        // Build a current_thread runtime and run it on a dedicated background thread.
        // This gives us minimal thread/memory overhead while still allowing rt.spawn()
        // futures to actually make progress.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime");

        let handle = rt.handle().clone();

        let rt_thread = std::thread::Builder::new()
            .name("ibex-tokio".into())
            .stack_size(512 * 1024) // 512 KB -- plenty for async I/O, saves ~7.5 MB vs default 8 MB
            .spawn(move || {
                // block_on a future that never completes -- keeps the runtime
                // alive and polling spawned tasks until the thread is dropped.
                rt.block_on(std::future::pending::<()>());
            })
            .expect("Failed to spawn tokio runtime thread");

        Self {
            connection_string: "mongodb://localhost:27017".into(),
            script: "show dbs".into(),
            shared: Arc::new(Mutex::new(SharedState {
                result_lines: Arc::new(Vec::new()),
                is_running: false,
                status_message: "Not connected".into(),
                connected: false,
                generation: 0,
            })),
            connection: Arc::new(tokio::sync::Mutex::new(None)),
            rt_handle: handle,
            _rt_thread: rt_thread,
            result_lines_cache: Arc::new(Vec::new()),
            status_cache: "Not connected".into(),
            is_running_cache: false,
            connected_cache: false,
            last_generation: 0,
            script_highlight_cache: None,
            script_hash: 0,
        }
    }
}

impl IbexApp {
    fn connect(&mut self) {
        let conn_str = self.connection_string.clone();
        let shared = Arc::clone(&self.shared);
        let connection = Arc::clone(&self.connection);

        {
            let mut s = shared.lock().unwrap();
            s.is_running = true;
            s.status_message = "Connecting...".into();
            s.generation += 1;
        }

        self.rt_handle.spawn(async move {
            match ClientOptions::parse(&conn_str).await {
                Ok(mut opts) => {
                    // Limit connection pool for minimal memory footprint
                    opts.max_pool_size = Some(2);
                    opts.min_pool_size = Some(0);
                    match Client::with_options(opts) {
                        Ok(client) => match client.list_database_names().await {
                            Ok(_) => {
                                *connection.lock().await = Some(MongoConnection {
                                    client,
                                    current_db: String::new(),
                                });
                                let mut s = shared.lock().unwrap();
                                s.status_message = format!("Connected to {conn_str}");
                                s.is_running = false;
                                s.connected = true;
                                s.generation += 1;
                            }
                            Err(e) => {
                                let mut s = shared.lock().unwrap();
                                s.status_message = format!("Connection failed: {e}");
                                s.is_running = false;
                                s.connected = false;
                                s.generation += 1;
                            }
                        },
                        Err(e) => {
                            let mut s = shared.lock().unwrap();
                            s.status_message = format!("Client error: {e}");
                            s.is_running = false;
                            s.connected = false;
                            s.generation += 1;
                        }
                    }
                }
                Err(e) => {
                    let mut s = shared.lock().unwrap();
                    s.status_message = format!("Parse error: {e}");
                    s.is_running = false;
                    s.connected = false;
                    s.generation += 1;
                }
            }
        });
    }

    fn disconnect(&mut self) {
        let connection = Arc::clone(&self.connection);
        self.rt_handle.spawn(async move {
            *connection.lock().await = None;
        });
        let mut s = self.shared.lock().unwrap();
        s.connected = false;
        s.status_message = "Disconnected".into();
        s.result_lines = Arc::new(Vec::new());
        s.generation += 1;
    }

    fn execute_script(&mut self) {
        let script = self.script.clone();
        let shared = Arc::clone(&self.shared);
        let connection = Arc::clone(&self.connection);

        let ops = match parse_script(&script) {
            Ok(ops) => ops,
            Err(e) => {
                let mut s = shared.lock().unwrap();
                s.result_lines = Arc::new(vec![format!("Parse error: {e}")]);
                s.generation += 1;
                return;
            }
        };

        {
            let mut s = shared.lock().unwrap();
            s.is_running = true;
            s.status_message = "Executing...".into();
            s.generation += 1;
        }

        self.rt_handle.spawn(async move {
            let mut conn_guard = connection.lock().await;
            if let Some(ref mut conn) = *conn_guard {
                let results = execute_ops(conn, ops).await;
                // Build flat line Vec directly from results, then drop results
                // to free all intermediate memory before storing.
                let mut all_lines = Vec::new();
                for (i, r) in results.iter().enumerate() {
                    if i > 0 {
                        all_lines.push(String::new());
                    }
                    all_lines.extend_from_slice(&r.lines);
                    all_lines.push(format!("  ({}ms)", r.elapsed.as_millis()));
                }
                drop(results); // free QueryResult memory before locking shared state
                let mut s = shared.lock().unwrap();
                s.result_lines = Arc::new(all_lines);
                s.is_running = false;
                let db_name = &conn.current_db;
                if db_name.is_empty() {
                    s.status_message = "Connected | no database selected".into();
                } else {
                    s.status_message = format!("Connected | db: {db_name}");
                }
                s.generation += 1;
            } else {
                let mut s = shared.lock().unwrap();
                s.result_lines = Arc::new(vec!["Error: Not connected to MongoDB".into()]);
                s.is_running = false;
                s.generation += 1;
            }
        });
    }

    /// Only copy from shared state when generation has changed -- avoids per-frame cloning.
    fn sync_from_shared(&mut self) {
        if let Ok(s) = self.shared.try_lock() {
            // Always sync these lightweight fields
            self.is_running_cache = s.is_running;
            self.connected_cache = s.connected;

            // Only update when data actually changed -- Arc::clone is just a refcount bump
            if s.generation != self.last_generation {
                self.result_lines_cache = Arc::clone(&s.result_lines);
                self.status_cache.clone_from(&s.status_message);
                self.last_generation = s.generation;
            }
        }
    }

    /// Simple FNV-1a hash for cache invalidation (fast, no allocations).
    fn hash_str(s: &str) -> u64 {
        let mut h: u64 = 0xcbf29ce484222325;
        for &b in s.as_bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        h
    }
}

impl eframe::App for IbexApp {
    fn logic(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.sync_from_shared();

        if self.is_running_cache {
            ctx.request_repaint();
        }

        if self.connected_cache
            && !self.is_running_cache
            && ctx.input(|i| i.key_pressed(egui::Key::Enter) && i.modifiers.command)
        {
            self.execute_script();
        }
    }

    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let mono = egui::FontId::monospace(13.0);

        // ── Top panel: connection bar ──
        egui::Panel::top("connection_bar").show_inside(ui, |ui| {
            ui.add_space(4.0);
            ui.horizontal(|ui| {
                ui.label("Connection:");
                let conn_edit = egui::TextEdit::singleline(&mut self.connection_string)
                    .desired_width(ui.available_width() - 180.0)
                    .hint_text("mongodb://localhost:27017")
                    .font(egui::TextStyle::Monospace);
                ui.add(conn_edit);

                if self.connected_cache {
                    if ui.button("Disconnect").clicked() {
                        self.disconnect();
                    }
                } else {
                    let btn =
                        ui.add_enabled(!self.is_running_cache, egui::Button::new("Connect"));
                    if btn.clicked() {
                        self.connect();
                    }
                }
            });
            ui.add_space(2.0);
        });

        // ── Bottom panel: status bar ──
        egui::Panel::bottom("status_bar").show_inside(ui, |ui| {
            ui.horizontal(|ui| {
                let color = if self.connected_cache {
                    egui::Color32::from_rgb(80, 200, 120)
                } else {
                    egui::Color32::from_rgb(200, 80, 80)
                };
                ui.label(egui::RichText::new("●").color(color).size(10.0));
                ui.label(&self.status_cache);
                if self.is_running_cache {
                    ui.spinner();
                }
            });
        });

        // ── Central panel: editor + results ──
        egui::CentralPanel::default().show_inside(ui, |ui| {
            // Toolbar
            ui.horizontal(|ui| {
                ui.heading("Script");
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    let run = ui.add_enabled(
                        self.connected_cache && !self.is_running_cache,
                        egui::Button::new("▶ Run  (Ctrl+Enter)"),
                    );
                    if run.clicked() {
                        self.execute_script();
                    }
                    if ui.button("Clear Results").clicked() {
                        let mut s = self.shared.lock().unwrap();
                        s.result_lines = Arc::new(Vec::new());
                        s.generation += 1;
                    }
                });
            });

            let available = ui.available_height();
            let editor_height = (available * 0.35).max(80.0);
            let results_height = available - editor_height - 30.0;

            // ── Script editor with cached syntax highlighting ──
            {
                let script_font = mono.clone();
                // Compute hash of current script text for cache invalidation
                let new_hash = Self::hash_str(&self.script);
                if new_hash != self.script_hash {
                    self.script_hash = new_hash;
                    let job = highlight_script(&self.script, &script_font);
                    self.script_highlight_cache = Some((new_hash, job));
                }
                // Clone the cached LayoutJob for the layouter (cheap -- just Arc bumps inside egui)
                let cached_job = self.script_highlight_cache.as_ref().map(|(_, j)| j.clone());
                egui::ScrollArea::vertical()
                    .id_salt("script_scroll")
                    .max_height(editor_height)
                    .show(ui, |ui| {
                        let mut layouter = |ui: &egui::Ui, text: &dyn egui::TextBuffer, _wrap_width: f32| {
                            // If text matches our cache, use it; otherwise re-highlight
                            let job = if let Some(ref cj) = cached_job {
                                cj.clone()
                            } else {
                                highlight_script(text.as_str(), &script_font)
                            };
                            ui.ctx().fonts_mut(|f| f.layout_job(job))
                        };
                        ui.add(
                            egui::TextEdit::multiline(&mut self.script)
                                .desired_width(f32::INFINITY)
                                .desired_rows(8)
                                .font(egui::TextStyle::Monospace)
                                .hint_text("use mydb;\ndb.mycollection.find()")
                                .layouter(&mut layouter),
                        );
                    });
            }

            ui.separator();

            // ── Results with virtualized rendering ──
            ui.horizontal(|ui| {
                ui.heading("Results");
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.label(
                        egui::RichText::new(format!("{} lines", self.result_lines_cache.len()))
                            .small()
                            .weak(),
                    );
                });
            });

            let total_lines = self.result_lines_cache.len();
            let total_height = total_lines as f32 * RESULT_LINE_HEIGHT;
            let results_font = mono;

            egui::ScrollArea::vertical()
                .id_salt("results_scroll")
                .max_height(results_height)
                .show(ui, |ui| {
                    if total_lines == 0 {
                        ui.weak("(no results)");
                        return;
                    }

                    // Allocate the full virtual height so the scrollbar is accurate
                    let (rect, _response) = ui.allocate_exact_size(
                        egui::vec2(ui.available_width(), total_height),
                        egui::Sense::hover(),
                    );

                    // Determine which lines are visible in the current viewport
                    let clip = ui.clip_rect();
                    let visible_top = (clip.top() - rect.top()).max(0.0);
                    let visible_bottom = (clip.bottom() - rect.top()).max(0.0);

                    let first_line = ((visible_top / RESULT_LINE_HEIGHT) as usize).saturating_sub(OVERSCAN_LINES);
                    let last_line = (((visible_bottom / RESULT_LINE_HEIGHT).ceil() as usize) + OVERSCAN_LINES)
                        .min(total_lines);

                    // Paint only visible lines
                    let painter = ui.painter_at(rect);
                    let left_x = rect.left() + 4.0;

                    for line_idx in first_line..last_line {
                        let y = rect.top() + line_idx as f32 * RESULT_LINE_HEIGHT;
                        let line = &self.result_lines_cache[line_idx];

                        // Build a tiny LayoutJob for just this one line
                        let mut job = LayoutJob::default();
                        highlight_json_line(line, &results_font, &mut job);

                        let galley = ui.ctx().fonts_mut(|f| f.layout_job(job));
                        painter.galley(egui::pos2(left_x, y), galley, COL_DEFAULT);
                    }
                });
        });
    }
}

// ─── Entry point ─────────────────────────────────────────────────────────────

fn main() -> eframe::Result<()> {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([900.0, 650.0])
            .with_min_inner_size([600.0, 400.0])
            .with_title("Ibex - MongoDB Client"),
        ..Default::default()
    };

    eframe::run_native(
        "Ibex",
        options,
        Box::new(|cc| {
            cc.egui_ctx.set_visuals(egui::Visuals::dark());
            Ok(Box::new(IbexApp::default()))
        }),
    )
}
