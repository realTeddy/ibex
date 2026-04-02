use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use eframe::egui;
use eframe::egui::text::LayoutJob;
use mongodb::bson::{Bson, Document};
use mongodb::Client;
use mongodb::options::ClientOptions;
use serde_json::Value as JsonValue;
use tokio::runtime::Handle;

// ─── Async MongoDB backend ───────────────────────────────────────────────────

struct MongoConnection {
    client: Client,
    current_db: String,
}

unsafe impl Send for MongoConnection {}

struct QueryResult {
    output: String,
    elapsed: Duration,
    _is_error: bool,
}

enum MongoOp {
    UseDb(String),
    Find {
        collection: String,
        filter: Option<String>,
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
    let after_db = &stmt[3..];
    let dot_pos = after_db
        .find('.')
        .ok_or("Expected db.<collection>.<method>(...)")?;
    let collection = after_db[..dot_pos].to_string();
    let rest = &after_db[dot_pos + 1..];

    let paren_pos = rest
        .find('(')
        .ok_or("Expected method call with parentheses")?;
    let method = &rest[..paren_pos];
    let args_raw = &rest[paren_pos..];

    if !args_raw.starts_with('(') || !args_raw.ends_with(')') {
        return Err("Malformed method call".into());
    }
    let args = args_raw[1..args_raw.len() - 1].trim();

    match method {
        "find" => Ok(MongoOp::Find {
            collection,
            filter: if args.is_empty() {
                None
            } else {
                Some(args.to_string())
            },
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
            })
        }
        _ => Err(format!("Unsupported method: {method}")),
    }
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
    use futures::TryStreamExt;

    let mut results = Vec::new();

    for op in ops {
        let start = Instant::now();
        let result = match op {
            MongoOp::UseDb(name) => {
                conn.current_db = name.clone();
                QueryResult {
                    output: format!("switched to db: {name}"),
                    elapsed: start.elapsed(),
                    _is_error: false,
                }
            }
            MongoOp::ShowDbs => match conn.client.list_database_names().await {
                Ok(names) => QueryResult {
                    output: names.join("\n"),
                    elapsed: start.elapsed(),
                    _is_error: false,
                },
                Err(e) => QueryResult {
                    output: format!("Error: {e}"),
                    elapsed: start.elapsed(),
                    _is_error: true,
                },
            },
            MongoOp::ShowCollections => {
                if conn.current_db.is_empty() {
                    QueryResult {
                        output: "Error: no database selected. Use `use <db>` first.".into(),
                        elapsed: start.elapsed(),
                        _is_error: true,
                    }
                } else {
                    let db = conn.client.database(&conn.current_db);
                    match db.list_collection_names().await {
                        Ok(names) => QueryResult {
                            output: if names.is_empty() {
                                "(no collections)".into()
                            } else {
                                names.join("\n")
                            },
                            elapsed: start.elapsed(),
                            _is_error: false,
                        },
                        Err(e) => QueryResult {
                            output: format!("Error: {e}"),
                            elapsed: start.elapsed(),
                            _is_error: true,
                        },
                    }
                }
            }
            MongoOp::Find { collection, filter } => {
                if conn.current_db.is_empty() {
                    QueryResult {
                        output: "Error: no database selected. Use `use <db>` first.".into(),
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
                                    output: format!("Error parsing filter: {e}"),
                                    elapsed: start.elapsed(),
                                    _is_error: true,
                                });
                                continue;
                            }
                        },
                        None => Document::new(),
                    };
                    match coll.find(filter_doc).await {
                        Ok(mut cursor) => {
                            let mut docs = Vec::new();
                            while let Ok(Some(doc)) = cursor.try_next().await {
                                docs.push(doc);
                                if docs.len() >= 100 {
                                    break;
                                }
                            }
                            let formatted = format_documents(&docs);
                            QueryResult {
                                output: format!("{formatted}\n\n({} document(s))", docs.len()),
                                elapsed: start.elapsed(),
                                _is_error: false,
                            }
                        }
                        Err(e) => QueryResult {
                            output: format!("Error: {e}"),
                            elapsed: start.elapsed(),
                            _is_error: true,
                        },
                    }
                }
            }
            MongoOp::CountDocuments { collection, filter } => {
                if conn.current_db.is_empty() {
                    QueryResult {
                        output: "Error: no database selected. Use `use <db>` first.".into(),
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
                                    output: format!("Error parsing filter: {e}"),
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
                            output: format!("{count}"),
                            elapsed: start.elapsed(),
                            _is_error: false,
                        },
                        Err(e) => QueryResult {
                            output: format!("Error: {e}"),
                            elapsed: start.elapsed(),
                            _is_error: true,
                        },
                    }
                }
            }
            MongoOp::InsertOne { collection, doc } => {
                if conn.current_db.is_empty() {
                    QueryResult {
                        output: "Error: no database selected. Use `use <db>` first.".into(),
                        elapsed: start.elapsed(),
                        _is_error: true,
                    }
                } else {
                    let db = conn.client.database(&conn.current_db);
                    let coll = db.collection::<Document>(&collection);
                    match relaxed_json_to_doc(&doc) {
                        Ok(document) => match coll.insert_one(document).await {
                            Ok(r) => QueryResult {
                                output: format!("Inserted document with _id: {}", r.inserted_id),
                                elapsed: start.elapsed(),
                                _is_error: false,
                            },
                            Err(e) => QueryResult {
                                output: format!("Error: {e}"),
                                elapsed: start.elapsed(),
                                _is_error: true,
                            },
                        },
                        Err(e) => QueryResult {
                            output: format!("Error parsing document: {e}"),
                            elapsed: start.elapsed(),
                            _is_error: true,
                        },
                    }
                }
            }
            MongoOp::DeleteOne { collection, filter } => {
                if conn.current_db.is_empty() {
                    QueryResult {
                        output: "Error: no database selected. Use `use <db>` first.".into(),
                        elapsed: start.elapsed(),
                        _is_error: true,
                    }
                } else {
                    let db = conn.client.database(&conn.current_db);
                    let coll = db.collection::<Document>(&collection);
                    match relaxed_json_to_doc(&filter) {
                        Ok(f) => match coll.delete_one(f).await {
                            Ok(r) => QueryResult {
                                output: format!("Deleted {} document(s)", r.deleted_count),
                                elapsed: start.elapsed(),
                                _is_error: false,
                            },
                            Err(e) => QueryResult {
                                output: format!("Error: {e}"),
                                elapsed: start.elapsed(),
                                _is_error: true,
                            },
                        },
                        Err(e) => QueryResult {
                            output: format!("Error parsing filter: {e}"),
                            elapsed: start.elapsed(),
                            _is_error: true,
                        },
                    }
                }
            }
            MongoOp::DeleteMany { collection, filter } => {
                if conn.current_db.is_empty() {
                    QueryResult {
                        output: "Error: no database selected. Use `use <db>` first.".into(),
                        elapsed: start.elapsed(),
                        _is_error: true,
                    }
                } else {
                    let db = conn.client.database(&conn.current_db);
                    let coll = db.collection::<Document>(&collection);
                    match relaxed_json_to_doc(&filter) {
                        Ok(f) => match coll.delete_many(f).await {
                            Ok(r) => QueryResult {
                                output: format!("Deleted {} document(s)", r.deleted_count),
                                elapsed: start.elapsed(),
                                _is_error: false,
                            },
                            Err(e) => QueryResult {
                                output: format!("Error: {e}"),
                                elapsed: start.elapsed(),
                                _is_error: true,
                            },
                        },
                        Err(e) => QueryResult {
                            output: format!("Error parsing filter: {e}"),
                            elapsed: start.elapsed(),
                            _is_error: true,
                        },
                    }
                }
            }
            MongoOp::Aggregate {
                collection,
                pipeline,
            } => {
                if conn.current_db.is_empty() {
                    QueryResult {
                        output: "Error: no database selected. Use `use <db>` first.".into(),
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
                    match pipeline_val {
                        Ok(val) => match json_value_to_bson_array(&val) {
                            Ok(stages) => match coll.aggregate(stages).await {
                                Ok(mut cursor) => {
                                    let mut docs = Vec::new();
                                    while let Ok(Some(doc)) = cursor.try_next().await {
                                        docs.push(doc);
                                        if docs.len() >= 100 {
                                            break;
                                        }
                                    }
                                    let formatted = format_documents(&docs);
                                    QueryResult {
                                        output: format!(
                                            "{formatted}\n\n({} document(s))",
                                            docs.len()
                                        ),
                                        elapsed: start.elapsed(),
                                        _is_error: false,
                                    }
                                }
                                Err(e) => QueryResult {
                                    output: format!("Error: {e}"),
                                    elapsed: start.elapsed(),
                                    _is_error: true,
                                },
                            },
                            Err(e) => QueryResult {
                                output: format!("Error parsing pipeline: {e}"),
                                elapsed: start.elapsed(),
                                _is_error: true,
                            },
                        },
                        Err(e) => QueryResult {
                            output: format!("Error parsing pipeline JSON: {e}"),
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

/// Pretty-print BSON documents as JSON -- single-pass, no intermediate String round-trip.
fn format_documents(docs: &[Document]) -> String {
    if docs.is_empty() {
        return "(no results)".to_string();
    }
    let mut out = String::with_capacity(docs.len() * 256);
    for (i, doc) in docs.iter().enumerate() {
        if i > 0 {
            out.push_str("\n---\n");
        }
        // Bson implements Serialize; go directly Bson → serde_json::Value (no String round-trip)
        let json_val: JsonValue = serde_json::to_value(Bson::Document(doc.clone()))
            .unwrap_or_else(|_| JsonValue::String(format!("{doc:?}")));
        // Write pretty JSON directly into our output buffer
        let pretty =
            serde_json::to_string_pretty(&json_val).unwrap_or_else(|_| format!("{doc:?}"));
        out.push_str(&pretty);
    }
    out
}

// ─── Syntax highlighting ─────────────────────────────────────────────────────

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
];

/// Syntax-highlight a mongo shell script for the editor.
fn highlight_script(text: &str, font_id: egui::FontId) -> LayoutJob {
    let mut job = LayoutJob::default();
    let chars: Vec<char> = text.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        let ch = chars[i];

        // Strings
        if ch == '"' || ch == '\'' {
            let start = i;
            let quote = ch;
            i += 1;
            while i < len && chars[i] != quote {
                if chars[i] == '\\' && i + 1 < len {
                    i += 1;
                }
                i += 1;
            }
            if i < len {
                i += 1;
            }
            let s: String = chars[start..i].iter().collect();
            append_colored(&mut job, &s, COL_STRING, font_id.clone());
            continue;
        }

        // "db" prefix
        if ch == 'd' && i + 1 < len && chars[i + 1] == 'b' && (i + 2 >= len || chars[i + 2] == '.') {
            append_colored(&mut job, "db", COL_DB, font_id.clone());
            i += 2;
            continue;
        }

        // Words (keywords, methods, identifiers)
        if ch.is_alphabetic() || ch == '_' || ch == '$' {
            let start = i;
            while i < len && (chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '$') {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            if MONGO_KEYWORDS.contains(&word.as_str()) {
                append_colored(&mut job, &word, COL_KEYWORD, font_id.clone());
            } else if MONGO_METHODS.contains(&word.as_str()) {
                append_colored(&mut job, &word, COL_METHOD, font_id.clone());
            } else if word == "true" || word == "false" {
                append_colored(&mut job, &word, COL_BOOL, font_id.clone());
            } else if word == "null" {
                append_colored(&mut job, &word, COL_NULL, font_id.clone());
            } else {
                append_colored(&mut job, &word, COL_DEFAULT, font_id.clone());
            }
            continue;
        }

        // Numbers
        if ch.is_ascii_digit() || (ch == '-' && i + 1 < len && chars[i + 1].is_ascii_digit()) {
            let start = i;
            if ch == '-' {
                i += 1;
            }
            while i < len && (chars[i].is_ascii_digit() || chars[i] == '.') {
                i += 1;
            }
            let num: String = chars[start..i].iter().collect();
            append_colored(&mut job, &num, COL_NUMBER, font_id.clone());
            continue;
        }

        // Braces/brackets/parens
        if matches!(ch, '{' | '}' | '[' | ']' | '(' | ')') {
            append_colored(&mut job, &ch.to_string(), COL_BRACE, font_id.clone());
            i += 1;
            continue;
        }

        // Everything else (dots, colons, semicolons, whitespace)
        append_colored(&mut job, &ch.to_string(), COL_DEFAULT, font_id.clone());
        i += 1;
    }

    job
}

/// Syntax-highlight JSON result text.
fn highlight_json(text: &str, font_id: egui::FontId) -> LayoutJob {
    let mut job = LayoutJob::default();
    let chars: Vec<char> = text.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        let ch = chars[i];

        // Separator lines "---"
        if ch == '-' && i + 2 < len && chars[i + 1] == '-' && chars[i + 2] == '-' {
            let start = i;
            while i < len && chars[i] == '-' {
                i += 1;
            }
            let s: String = chars[start..i].iter().collect();
            append_colored(&mut job, &s, COL_SEPARATOR, font_id.clone());
            continue;
        }

        // Strings -- detect if this is a JSON key (followed by ':')
        if ch == '"' {
            let start = i;
            i += 1;
            while i < len && chars[i] != '"' {
                if chars[i] == '\\' && i + 1 < len {
                    i += 1;
                }
                i += 1;
            }
            if i < len {
                i += 1;
            }
            let s: String = chars[start..i].iter().collect();

            // Look ahead for ':'
            let mut j = i;
            while j < len && chars[j].is_whitespace() && chars[j] != '\n' {
                j += 1;
            }
            if j < len && chars[j] == ':' {
                append_colored(&mut job, &s, COL_KEY, font_id.clone());
            } else {
                append_colored(&mut job, &s, COL_STRING, font_id.clone());
            }
            continue;
        }

        // Words: true, false, null
        if ch.is_alphabetic() {
            let start = i;
            while i < len && chars[i].is_alphanumeric() {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            match word.as_str() {
                "true" | "false" => {
                    append_colored(&mut job, &word, COL_BOOL, font_id.clone());
                }
                "null" => {
                    append_colored(&mut job, &word, COL_NULL, font_id.clone());
                }
                _ => {
                    append_colored(&mut job, &word, COL_DEFAULT, font_id.clone());
                }
            }
            continue;
        }

        // Numbers
        if ch.is_ascii_digit() || (ch == '-' && i + 1 < len && chars[i + 1].is_ascii_digit()) {
            let start = i;
            if ch == '-' {
                i += 1;
            }
            while i < len && (chars[i].is_ascii_digit() || chars[i] == '.' || chars[i] == 'e' || chars[i] == 'E' || chars[i] == '+' || chars[i] == '-') {
                // Avoid consuming a '-' that starts a new negative number after 'e'
                if (chars[i] == '-' || chars[i] == '+') && i > start && chars[i - 1] != 'e' && chars[i - 1] != 'E' {
                    break;
                }
                i += 1;
            }
            let num: String = chars[start..i].iter().collect();
            append_colored(&mut job, &num, COL_NUMBER, font_id.clone());
            continue;
        }

        // Braces
        if matches!(ch, '{' | '}' | '[' | ']') {
            append_colored(&mut job, &ch.to_string(), COL_BRACE, font_id.clone());
            i += 1;
            continue;
        }

        // Everything else
        append_colored(&mut job, &ch.to_string(), COL_DEFAULT, font_id.clone());
        i += 1;
    }

    job
}

fn append_colored(job: &mut LayoutJob, text: &str, color: egui::Color32, font_id: egui::FontId) {
    job.append(
        text,
        0.0,
        egui::TextFormat {
            font_id,
            color,
            ..Default::default()
        },
    );
}

// ─── GUI Application ─────────────────────────────────────────────────────────

struct SharedState {
    result_text: String,
    is_running: bool,
    status_message: String,
    connected: bool,
    /// Monotonic generation counter -- bumped every time result_text or status changes.
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
    // Cached copies -- only updated when generation changes
    result_cache: String,
    status_cache: String,
    is_running_cache: bool,
    connected_cache: bool,
    last_generation: u64,
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
                result_text: String::new(),
                is_running: false,
                status_message: "Not connected".into(),
                connected: false,
                generation: 0,
            })),
            connection: Arc::new(tokio::sync::Mutex::new(None)),
            rt_handle: handle,
            _rt_thread: rt_thread,
            result_cache: String::new(),
            status_cache: "Not connected".into(),
            is_running_cache: false,
            connected_cache: false,
            last_generation: 0,
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
        s.result_text.clear();
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
                s.result_text = format!("Parse error: {e}");
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
                let mut s = shared.lock().unwrap();
                let mut output = String::new();
                for r in &results {
                    if !output.is_empty() {
                        output.push_str("\n\n");
                    }
                    output.push_str(&r.output);
                    output.push_str(&format!("\n  ({}ms)", r.elapsed.as_millis()));
                }
                s.result_text = output;
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
                s.result_text = "Error: Not connected to MongoDB".into();
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

            // Only clone strings when data actually changed
            if s.generation != self.last_generation {
                self.result_cache.clone_from(&s.result_text);
                self.status_cache.clone_from(&s.status_message);
                self.last_generation = s.generation;
            }
        }
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
                        s.result_text.clear();
                        s.generation += 1;
                    }
                });
            });

            let available = ui.available_height();
            let editor_height = (available * 0.35).max(80.0);
            let results_height = available - editor_height - 30.0;

            // ── Script editor with syntax highlighting ──
            let script_font = mono.clone();
            egui::ScrollArea::vertical()
                .id_salt("script_scroll")
                .max_height(editor_height)
                .show(ui, |ui| {
                    let mut layouter = |ui: &egui::Ui, text: &dyn egui::TextBuffer, _wrap_width: f32| {
                        let layout_job = highlight_script(text.as_str(), script_font.clone());
                        ui.ctx().fonts_mut(|f| f.layout_job(layout_job))
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

            ui.separator();

            // ── Results with JSON syntax highlighting ──
            ui.horizontal(|ui| {
                ui.heading("Results");
            });

            let results_font = mono.clone();
            egui::ScrollArea::vertical()
                .id_salt("results_scroll")
                .max_height(results_height)
                .show(ui, |ui| {
                    let mut layouter = |ui: &egui::Ui, text: &dyn egui::TextBuffer, _wrap_width: f32| {
                        let layout_job = highlight_json(text.as_str(), results_font.clone());
                        ui.ctx().fonts_mut(|f| f.layout_job(layout_job))
                    };
                    // Pass &mut directly -- no clone. TextEdit won't mutate in
                    // practice since we don't process the output, and even if the
                    // user types into results it just modifies the cache which gets
                    // overwritten on next query.
                    ui.add(
                        egui::TextEdit::multiline(&mut self.result_cache)
                            .desired_width(f32::INFINITY)
                            .font(egui::TextStyle::Monospace)
                            .layouter(&mut layouter),
                    );
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
