use std::collections::{HashMap, HashSet};

use rusqlite::{Row, Transaction};
use uuid::Uuid;

use crate::{model::Node, DocumentDb, InferredType, Selector};

pub struct IgnoreRule {
    pub tag: String,
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

fn scrub_node(
    db: &DocumentDb,
    tx: &Transaction<'_>,
    node_id: usize,
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
        InferredType::Uuid => "00000000-0000-0000-0000-000000000000",
        InferredType::DateTime => "1970-01-01T00:00:00Z",
        InferredType::Time => "00:00:00",
        InferredType::Date => "1970-01-01",
        InferredType::Duration => "55:55:55",
        InferredType::Json => {
            return;
        }
    };

    tx.execute(
        "UPDATE nodes SET node_value = ?1 WHERE node_id = ?2",
        (value, node_id),
    )
    .unwrap();
}

fn mask_node(
    db: &DocumentDb,
    tx: &Transaction<'_>,
    node_id: usize,
    ty: InferredType,
    options: &Options,
    seed: Uuid,
) {
    if options.mask.uuids && matches!(ty, InferredType::Uuid) {
        let v = db.node_raw_value(node_id).unwrap().unwrap();
        let uuid = Uuid::new_v5(&seed, v.trim().as_bytes()).to_string();

        tx.execute(
            "UPDATE nodes SET node_value = ?1 WHERE node_id = ?2",
            (uuid, node_id),
        )
        .unwrap();
    }
}

fn scrub_attr(
    db: &DocumentDb,
    tx: &Transaction<'_>,
    attr_id: usize,
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
        InferredType::Uuid => "00000000-0000-0000-0000-000000000000",
        InferredType::DateTime => "1970-01-01T00:00:00Z",
        InferredType::Time => "00:00:00",
        InferredType::Date => "1970-01-01",
        InferredType::Duration => "55:55:55",
        InferredType::Json => {
            return;
        }
    };

    tx.execute(
        "UPDATE attrs SET attr_value = ?1 WHERE attr_id = ?2",
        (value, attr_id),
    )
    .unwrap();
}

fn mask_attr(
    db: &DocumentDb,
    tx: &Transaction<'_>,
    attr_id: usize,
    ty: InferredType,
    options: &Options,
    seed: Uuid,
) {
    if options.mask.uuids && matches!(ty, InferredType::Uuid) {
        let v = db.attr_raw_value(attr_id).unwrap().unwrap();
        let uuid = Uuid::new_v5(&seed, v.as_bytes()).to_string();

        tx.execute(
            "UPDATE attrs SET attr_value = ?1 WHERE attr_id = ?2",
            (uuid, attr_id),
        )
        .unwrap();
    }
}

pub fn redact(
    db: &DocumentDb,
    mut out_db: DocumentDb,
    options: &Options,
) -> Result<DocumentDb, rusqlite::Error> {
    eprintln!("Node count: {}", db.node_count()?);
    eprintln!("Attr count: {}", db.attr_count()?);

    let seed = Uuid::new_v4();

    let tx = out_db.conn.transaction().unwrap();
    let mut i = 0usize;
    for node in db.all_elements().unwrap() {
        i += 1;

        if i % 10000 == 0 {
            eprintln!("Parsed {i} nodes...");
        }

        let node = node.unwrap();

        let matched_rules = options
            .ignore
            .iter()
            .filter(|x| x.tag == node.name)
            .collect::<Vec<_>>();

        let ty = db.inferred_type(node.node_id).unwrap();
        mask_node(db, &tx, node.node_id, ty, options, seed);

        if matched_rules.is_empty() {
            scrub_node(&db, &tx, node.node_id, ty, options, seed);

            let child_nodes = db.child_nodes(node.node_id).unwrap();
            for child_node in child_nodes {
                let child_node = child_node.unwrap();
                let ty = db.inferred_type(node.node_id).unwrap();
                match child_node {
                    Node::Text(x) => {
                        mask_node(db, &tx, x.node_id, ty, options, seed);
                        scrub_node(&db, &tx, x.node_id, ty, options, seed);
                    }
                    Node::Comment(x) => {
                        mask_node(db, &tx, x.node_id, ty, options, seed);
                        scrub_node(&db, &tx, x.node_id, ty, options, seed);
                    }
                    Node::CData(x) => {
                        mask_node(db, &tx, x.node_id, ty, options, seed);
                        scrub_node(&db, &tx, x.node_id, ty, options, seed);
                    }
                    _ => {}
                }
            }

            let attrs = db.attrs(node.node_id).unwrap();
            for attr in attrs {
                let attr = attr.unwrap();
                let ty = db.attr_inferred_type(attr.attr_id).unwrap();
                mask_attr(db, &tx, attr.attr_id, ty, options, seed);
                scrub_attr(&db, &tx, attr.attr_id, ty, options, seed);
            }
        } else {
            for rule in matched_rules {
                if !rule.value {
                    scrub_node(&db, &tx, node.node_id, ty, options, seed);
                    let child_nodes = db.child_nodes(node.node_id).unwrap();
                    for child_node in child_nodes {
                        let child_node = child_node.unwrap();
                        let ty = db.inferred_type(child_node.node_id()).unwrap();
                        match child_node {
                            Node::Text(x) => {
                                mask_node(db, &tx, x.node_id, ty, options, seed);
                                scrub_node(&db, &tx, x.node_id, ty, options, seed);
                            }
                            Node::Comment(x) => {
                                mask_node(db, &tx, x.node_id, ty, options, seed);
                                scrub_node(&db, &tx, x.node_id, ty, options, seed);
                            }
                            Node::CData(x) => {
                                mask_node(db, &tx, x.node_id, ty, options, seed);
                                scrub_node(&db, &tx, x.node_id, ty, options, seed);
                            }
                            _ => {}
                        }
                    }
                }

                let attrs = db.attrs(node.node_id).unwrap();

                for attr in attrs {
                    let attr = attr.unwrap();
                    let ty = db.attr_inferred_type(attr.attr_id).unwrap();
                    if !rule.attrs.contains(&attr.name) {
                        mask_attr(db, &tx, attr.attr_id, ty, options, seed);
                        scrub_attr(&db, &tx, attr.attr_id, ty, options, seed);
                    }
                }
            }
        }
    }

    tx.commit().unwrap();
    Ok(out_db)
}
