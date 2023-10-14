use std::collections::HashSet;

use rusqlite::Transaction;
use uuid::Uuid;

use crate::{model::Node, DocumentDb, InferredType};

pub struct IgnoreRule {
    pub match_tag: String,
    pub allow_value: bool,
    pub allow_attrs: HashSet<String>,
}

pub struct Mask {
    pub uuids: bool,
}

pub struct Options {
    pub ignore: Vec<IgnoreRule>,
    pub mask: Mask,
    // replace: Replace,
}

#[derive(Debug, Clone, Copy)]
enum Id {
    Node(usize),
    Attr(usize),
}

fn handle_uuid(db: &DocumentDb, tx: &Transaction<'_>, id: Id, options: &Options, seed: Uuid) {
    if options.mask.uuids {
        let v = match id {
            Id::Node(node_id) => db.node_raw_value(node_id).unwrap().unwrap(),
            Id::Attr(attr_id) => db.attr_raw_value(attr_id).unwrap().unwrap(),
        };
        let uuid = Uuid::new_v5(&seed, v.trim().as_bytes()).to_string();
        set_value(id, tx, &uuid);
    } else {
        set_value(id, tx, "00000000-0000-0000-0000-000000000000");
    }
}

fn set_value(id: Id, tx: &Transaction<'_>, value: &str) {
    match id {
        Id::Node(node_id) => {
            tx.execute(
                "UPDATE nodes SET node_value = ?1 WHERE node_id = ?2",
                (value, node_id),
            )
            .unwrap();
        }
        Id::Attr(attr_id) => {
            tx.execute(
                "UPDATE attrs SET attr_value = ?1 WHERE attr_id = ?2",
                (value, attr_id),
            )
            .unwrap();
        }
    }
}

fn scrub(
    db: &DocumentDb,
    tx: &Transaction<'_>,
    id: Id,
    ty: InferredType,
    options: &Options,
    seed: Uuid,
) {
    let value = match ty {
        InferredType::Empty | InferredType::Whitespace => {
            return;
        }
        InferredType::String => "[redacted]",
        InferredType::Boolean => {
            return;
        }
        InferredType::Int => "0",
        InferredType::Float => "0.123",
        InferredType::Uuid => return handle_uuid(db, tx, id, options, seed),
        InferredType::DateTime => "1970-01-01T00:00:00Z",
        InferredType::Time => "00:00:00",
        InferredType::Date => "1970-01-01",
        InferredType::Duration => "55:55:55",
        InferredType::Json => r#"{"[redacted]": true}"#,
    };

    set_value(id, tx, value);
}

pub fn redact(
    db: &DocumentDb,
    mut out_db: DocumentDb,
    options: &Options,
) -> Result<DocumentDb, rusqlite::Error> {
    eprintln!("Element count: {}", db.element_count()?);
    eprintln!("Node count: {}", db.node_count()?);
    eprintln!("Attr count: {}", db.attr_count()?);

    let seed = Uuid::new_v4();

    let tx = out_db.conn.transaction().unwrap();
    let mut i = 0usize;
    for node in db.all_elements().unwrap() {
        i += 1;

        if i % 10000 == 0 {
            eprintln!("Parsed {i} elements...");
        }

        let node = node.unwrap();

        let matched_rules = options
            .ignore
            .iter()
            .filter(|x| x.match_tag == node.name)
            .collect::<Vec<_>>();

        let ty = db.inferred_type(node.node_id).unwrap();

        if matched_rules.is_empty() {
            scrub(&db, &tx, Id::Node(node.node_id), ty, options, seed);

            let child_nodes = db.child_nodes(node.node_id).unwrap();
            for child_node in child_nodes {
                let child_node = child_node.unwrap();
                match child_node {
                    Node::Text(x) => {
                        let ty = db.inferred_type(x.node_id).unwrap();
                        scrub(db, &tx, Id::Node(x.node_id), ty, options, seed);
                    }
                    Node::Comment(x) => {
                        let ty = db.inferred_type(x.node_id).unwrap();
                        scrub(db, &tx, Id::Node(x.node_id), ty, options, seed);
                    }
                    Node::CData(x) => {
                        let ty = db.inferred_type(x.node_id).unwrap();
                        scrub(db, &tx, Id::Node(x.node_id), ty, options, seed);
                    }
                    _ => {}
                }
            }

            let attrs = db.attrs(node.node_id).unwrap();
            for attr in attrs {
                let attr = attr.unwrap();
                let ty = db.attr_inferred_type(attr.attr_id).unwrap();
                scrub(db, &tx, Id::Attr(attr.attr_id), ty, options, seed);
            }
        } else {
            for rule in matched_rules {
                let child_nodes = db.child_nodes(node.node_id).unwrap();
                if !rule.allow_value {
                    scrub(&db, &tx, Id::Node(node.node_id), ty, options, seed);
                }
                for child_node in child_nodes {
                    let child_node = child_node.unwrap();
                    match child_node {
                        Node::Text(x) => {
                            if !rule.allow_value {
                                let ty = db.inferred_type(x.node_id).unwrap();
                                scrub(&db, &tx, Id::Node(x.node_id), ty, options, seed);
                            }
                        }
                        Node::Comment(x) => {
                            if !rule.allow_value {
                                let ty = db.inferred_type(x.node_id).unwrap();
                                scrub(&db, &tx, Id::Node(x.node_id), ty, options, seed);
                            }
                        }
                        Node::CData(x) => {
                            if !rule.allow_value {
                                let ty = db.inferred_type(x.node_id).unwrap();
                                scrub(&db, &tx, Id::Node(x.node_id), ty, options, seed);
                            }
                        }
                        _ => {}
                    }
                }

                let attrs = db.attrs(node.node_id).unwrap();

                for attr in attrs {
                    let attr = attr.unwrap();

                    if !rule.allow_attrs.contains(&attr.name) {
                        let ty = db.attr_inferred_type(attr.attr_id).unwrap();
                        scrub(&db, &tx, Id::Attr(attr.attr_id), ty, options, seed);
                    }
                }
            }
        }
    }

    tx.commit().unwrap();
    Ok(out_db)
}
