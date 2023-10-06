use std::collections::{HashMap, HashSet};

use rusqlite::Row;
use uuid::Uuid;

use crate::{select, DocumentDb, Selector};

pub struct IgnoreRule {
    pub selector: Selector,
    pub value: bool,
    pub attrs: HashSet<String>,
}

pub struct Mask {
    pub uuids: bool,
}

pub struct Options {
    pub ignore: Vec<IgnoreRule>,
    pub mask: Mask,
    // replace: Replace,
}

pub fn redact(mut db: DocumentDb, options: &Options) -> Result<DocumentDb, rusqlite::Error> {
    let mut ignored = HashMap::new();

    options.ignore.iter().for_each(|rule| {
        let results = rule.selector.clone().match_all(&db).unwrap();
        results.into_iter().for_each(|x| {
            ignored.insert(x.node_id, rule);
        });
        // TODO: child nodes that are not elements if value = true
    });

    let ignored_keys = ignored
        .keys()
        .map(|x| x.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let mut ignored_attrs = HashSet::new();

    for (node_id, rule) in ignored {
        for attr_name in rule.attrs.iter() {
            if let Some(attr) = db.attr_by_name(node_id, &attr_name, None).unwrap() {
                ignored_attrs.insert(attr.attr_id);
            }
        }
    }

    let ignored_attr_keys = ignored_attrs
        .iter()
        .map(|x| x.to_string())
        .collect::<Vec<_>>()
        .join(",");

    let tx = db.conn.transaction().unwrap();

    let redaction_queries = [
        format!("UPDATE nodes SET node_value = '[redacted]' WHERE inferred_type = 'string' AND node_id NOT IN ({ignored_keys}) AND node_type < 5"),
        // format!("UPDATE nodes SET node_value = '[redacted]' WHERE inferred_type = 'uuid' AND node_id NOT IN ({ignored_keys})"),
        format!("UPDATE nodes SET node_value = '0' WHERE inferred_type = 'int' AND node_id NOT IN ({ignored_keys})"),
        format!("UPDATE nodes SET node_value = '0.123' WHERE inferred_type = 'float' AND node_id NOT IN ({ignored_keys})"),
        format!("UPDATE nodes SET node_value = '1970-01-01T00:00:00Z' WHERE inferred_type = 'datetime' AND node_id NOT IN ({ignored_keys})"),
        format!("UPDATE nodes SET node_value = '1970-01-01' WHERE inferred_type = 'date' AND node_id NOT IN ({ignored_keys})"),
        format!("UPDATE nodes SET node_value = '00:00:00' WHERE inferred_type = 'time' AND node_id NOT IN ({ignored_keys})"),
        format!("UPDATE nodes SET node_value = '12:34:56' WHERE inferred_type = 'duration' AND node_id NOT IN ({ignored_keys})"),
        format!(r#"UPDATE nodes SET node_value = '{{"redacted": true}}' WHERE inferred_type = 'duration' AND node_id NOT IN ({ignored_keys})"#),
    ];

    for query in redaction_queries {
        tx.execute(
            &query,
            [],
        )?;
    }

    let attr_redaction_queries = [
        format!("UPDATE attrs SET attr_value = '[redacted]' WHERE inferred_type = 'string' AND attr_id NOT IN ({ignored_attr_keys})"),
        // format!("UPDATE attrs SET attr_value = '[redacted]' WHERE inferred_type = 'uuid' AND attr_id NOT IN ({ignored_attr_keys})"),
        format!("UPDATE attrs SET attr_value = '0' WHERE inferred_type = 'int' AND attr_id NOT IN ({ignored_attr_keys})"),
        format!("UPDATE attrs SET attr_value = '0.123' WHERE inferred_type = 'float' AND attr_id NOT IN ({ignored_attr_keys})"),
        format!("UPDATE attrs SET attr_value = '1970-01-01T00:00:00Z' WHERE inferred_type = 'datetime' AND attr_id NOT IN ({ignored_attr_keys})"),
        format!("UPDATE attrs SET attr_value = '1970-01-01' WHERE inferred_type = 'date' AND attr_id NOT IN ({ignored_attr_keys})"),
        format!("UPDATE attrs SET attr_value = '00:00:00' WHERE inferred_type = 'time' AND attr_id NOT IN ({ignored_attr_keys})"),
        format!("UPDATE attrs SET attr_value = '12:34:56' WHERE inferred_type = 'duration' AND attr_id NOT IN ({ignored_attr_keys})"),
        format!(r#"UPDATE attrs SET attr_value = '{{"redacted": true}}' WHERE inferred_type = 'duration' AND attr_id NOT IN ({ignored_attr_keys})"#),
    ];

    for query in attr_redaction_queries {
        tx.execute(
            &query,
            [],
        )?;
    }

    if options.mask.uuids {
        let random_ns = uuid::Uuid::new_v4();
        
        let stmt = tx.prepare("SELECT node_id, node_value FROM nodes WHERE inferred_type = 'uuid'")?;
        let result = stmt.query_map([], |r: &Row<'_>| {
            Ok((r.get::<_, usize>(0)?, r.get::<_, String>(1)?))
        })?;
        for row in result {
            let (node_id, uuid_str) = row.unwrap();
            let uuid = Uuid::parse_str(&uuid_str).unwrap();
            let new_uuid =  uuid::Uuid::new_v5(&random_ns, uuid.as_bytes());
            tx.execute(
                "UPDATE nodes SET node_value = ?1 WHERE node_id = ?2",
                (new_uuid.to_string(), node_id),
            )?;
        }

        let stmt = tx.prepare("SELECT attr_id, attr_value FROM attrs WHERE inferred_type = 'uuid'")?;
        let result = stmt.query_map([], |r: &Row<'_>| {
            Ok((r.get::<_, usize>(0)?, r.get::<_, String>(1)?))
        })?;
        for row in result {
            let (attr_id, uuid_str) = row.unwrap();
            let uuid = Uuid::parse_str(&uuid_str).unwrap();
            let new_uuid =  uuid::Uuid::new_v5(&random_ns, uuid.as_bytes());
            tx.execute(
                "UPDATE attrs SET attr_value = ?1 WHERE attr_id = ?2",
                (new_uuid.to_string(), attr_id),
            )?;
        }
    }

    tx.commit().unwrap();

    // TODO: deal with each type's concept of a default value.
    
    // db.conn.execute(
    //     "UPDATE attrs SET attr_value = '[redacted]' WHERE inferred_type = 'string'",
    //     [],
    // )?;

    // Find all UUIDs, replace with a UUIDv5 seeded with crime
    Ok(db)
}
