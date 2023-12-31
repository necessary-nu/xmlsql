use std::{
    borrow::Cow,
    path::{Path, PathBuf},
};

use rusqlite::{types::ToSqlOutput, Batch, OpenFlags, OptionalExtension, Result, Row};

use crate::{
    infer::InferredType,
    model,
    writer::{Config, Print, State},
    ParseOptions,
};

#[derive(Debug)]
enum Mode {
    InMemory,
    OnDisk,
    TempDir(tempfile::TempDir),
}

#[derive(Debug)]
pub struct DocumentDb {
    pub(crate) conn: rusqlite::Connection,
    pub(crate) options: ParseOptions,
    _mode: Mode,
}

const PRAGMAS: &str = r#"
PRAGMA journal_mode = OFF;
PRAGMA synchronous = 0;
PRAGMA cache_size = 1000000;
PRAGMA locking_mode = EXCLUSIVE;
PRAGMA temp_store = MEMORY;
PRAGMA page_size = 65536;
"#;

const SQL_SIMPLE: &str = r#"
CREATE TABLE nodes (
    node_id INTEGER PRIMARY KEY,
    parent_node_id INTEGER NOT NULL,
    node_order INTEGER NOT NULL,

    node_type INTEGER NOT NULL,
    node_ns TEXT,
    node_name TEXT,
    node_value TEXT,

    buffer_position INTEGER NOT NULL,
    FOREIGN KEY (parent_node_id) REFERENCES nodes(node_id)
);

CREATE TABLE attrs (
    attr_id INTEGER PRIMARY KEY,
    attr_order INTEGER NOT NULL,
    attr_ns TEXT,
    attr_name TEXT NOT NULL,
    attr_value TEXT NOT NULL,

    parent_node_id INTEGER NOT NULL,
    buffer_position INTEGER NOT NULL,

    FOREIGN KEY(parent_node_id) REFERENCES nodes(node_id)
);

INSERT INTO nodes (node_id, parent_node_id, node_order, node_type, node_ns, node_name, node_value, buffer_position)
VALUES
    (0, 0, 0, 0, NULL, NULL, NULL, 0),
    (1, 0, 0, 1, NULL, NULL, NULL, 0);
"#;

const SQL_WITH_TYPES: &str = r#"
CREATE TABLE nodes (
    node_id INTEGER PRIMARY KEY,
    parent_node_id INTEGER NOT NULL,
    node_order INTEGER NOT NULL,

    node_type INTEGER NOT NULL,
    node_ns TEXT,
    node_name TEXT,
    node_value TEXT,

    buffer_position INTEGER NOT NULL,
    inferred_type TEXT NOT NULL,
    FOREIGN KEY (parent_node_id) REFERENCES nodes(node_id)
);

CREATE TABLE attrs (
    attr_id INTEGER PRIMARY KEY,
    attr_order INTEGER NOT NULL,
    attr_ns TEXT,
    attr_name TEXT NOT NULL,
    attr_value TEXT NOT NULL,

    parent_node_id INTEGER NOT NULL,
    buffer_position INTEGER NOT NULL,
    inferred_type TEXT NOT NULL,

    FOREIGN KEY(parent_node_id) REFERENCES nodes(node_id)
);

INSERT INTO nodes (node_id, parent_node_id, node_order, node_type, node_ns, node_name, node_value, buffer_position, inferred_type)
VALUES
    (0, 0, 0, 0, NULL, NULL, NULL, 0, 'empty'),
    (1, 0, 0, 1, NULL, NULL, NULL, 0, 'empty');
"#;

impl DocumentDb {
    pub(crate) fn create_in_memory(options: ParseOptions) -> Result<Self> {
        let conn = rusqlite::Connection::open_in_memory()?;
        Self::_create(conn, Mode::InMemory, options)
    }

    pub(crate) fn create_temp(options: ParseOptions) -> Result<Self> {
        let tmp = tempfile::tempdir()
            .map_err(|_| rusqlite::Error::InvalidPath(PathBuf::from(":temp:")))?;
        let conn = rusqlite::Connection::open(tmp.path().join("db"))?;
        Self::_create(conn, Mode::TempDir(tmp), options)
    }

    pub(crate) fn create<P: AsRef<Path>>(path: P, options: ParseOptions) -> Result<Self> {
        let conn = rusqlite::Connection::open_with_flags(
            path.as_ref(),
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )?;
        Self::_create(conn, Mode::OnDisk, options)
    }

    fn _create(conn: rusqlite::Connection, mode: Mode, options: ParseOptions) -> Result<Self> {
        conn.execute_batch(PRAGMAS)?;

        let mut batch = Batch::new(
            &conn,
            if options.infer_types {
                SQL_WITH_TYPES
            } else {
                SQL_SIMPLE
            },
        );

        while let Some(mut stmt) = batch.next()? {
            stmt.execute([])?;
        }

        Ok(Self {
            conn,
            options,
            _mode: mode,
        })
    }

    pub fn open<P: AsRef<Path>>(path: P, options: ParseOptions) -> Result<Self> {
        let conn = rusqlite::Connection::open_with_flags(
            path.as_ref(),
            OpenFlags::SQLITE_OPEN_READ_WRITE,
        )?;
        Ok(Self {
            conn,
            options,
            _mode: Mode::OnDisk,
        })
    }

    pub fn element_count(&self) -> Result<usize> {
        let count = self.conn.query_row(
            r#"
                SELECT COUNT(*) FROM nodes WHERE node_type = 1;
            "#,
            [],
            |r| r.get::<_, usize>(0),
        )?;
        Ok(count)
    }

    pub fn node_count(&self) -> Result<usize> {
        let count = self.conn.query_row(
            r#"
                SELECT COUNT(*) FROM nodes
            "#,
            [],
            |r| r.get::<_, usize>(0),
        )?;
        Ok(count)
    }

    pub fn attr_count(&self) -> Result<usize> {
        let count = self.conn.query_row(
            r#"
                SELECT COUNT(*) FROM attrs
            "#,
            [],
            |r| r.get::<_, usize>(0),
        )?;
        Ok(count)
    }

    pub fn parent_element_id(&self, node_id: usize) -> Result<usize> {
        let node_id = self.conn.query_row(
            r#"
                SELECT parent_node_id FROM nodes WHERE node_id = ?1
            "#,
            [node_id],
            |r| r.get::<_, usize>(0),
        )?;
        Ok(node_id)
    }

    pub fn prev_sibling_element_id(&self, node_id: usize) -> Result<usize> {
        let node_id = self.conn.query_row(
            r#"
                SELECT parent_node_id FROM nodes WHERE parent_node_id = (SELECT parent_node_id
                    FROM nodes
                    WHERE node_id = ?1)
                AND node_order < (SELECT node_order FROM nodes WHERE node_id = ?1)
                ORDER BY node_order DESC LIMIT 1;
            "#,
            [node_id],
            |r| r.get::<_, usize>(0),
        )?;
        Ok(node_id)
    }

    pub fn next_sibling_element_id(&self, node_id: usize) -> Result<usize> {
        let node_id = self.conn.query_row(
            r#"
                SELECT parent_node_id FROM nodes WHERE parent_node_id = (SELECT parent_node_id
                    FROM nodes
                    WHERE node_id = ?1)
                AND node_order > (SELECT node_order FROM nodes WHERE node_id = ?1)
                ORDER BY node_order ASC LIMIT 1;
            "#,
            [node_id],
            |r| r.get::<_, usize>(0),
        )?;
        Ok(node_id)
    }

    pub fn element(&self, node_id: usize) -> Result<model::Element> {
        self.conn.query_row(
            r#"
                SELECT node_ns, node_name FROM nodes WHERE node_id = ?1 AND node_type = 1
            "#,
            [node_id],
            |r| {
                Ok(model::Element {
                    node_id,
                    ns: r.get::<_, Option<String>>(0)?,
                    name: r.get::<_, String>(1)?,
                })
            },
        )
    }

    pub fn node(&self, node_id: usize) -> Result<model::Node> {
        let raw_node = self.conn.query_row(
            r#"
                SELECT node_type, node_ns, node_name, node_value FROM nodes WHERE node_id = ?1
            "#,
            [node_id],
            |r| {
                Ok(model::RawNode {
                    node_id,
                    node_type: NodeType::try_from(r.get::<_, u8>(0)?).unwrap(),
                    ns: r.get::<_, Option<String>>(1)?,
                    name: r.get::<_, Option<String>>(2)?,
                    value: r.get::<_, Option<String>>(3)?,
                })
            },
        )?;

        Ok(raw_node.into())
    }

    pub fn buffer_position(&self, node_id: usize) -> Result<u64> {
        let pos = self.conn.query_row(
            r#"
                SELECT buffer_position FROM nodes WHERE node_id = ?1
            "#,
            [node_id],
            |r| r.get::<_, u64>(0),
        )?;

        Ok(pos)
    }

    pub fn attr_buffer_position(&self, attr_id: usize) -> Result<u64> {
        let pos = self.conn.query_row(
            r#"
                SELECT buffer_position FROM attrs WHERE attr_id = ?1
            "#,
            [attr_id],
            |r| r.get::<_, u64>(0),
        )?;

        Ok(pos)
    }

    pub fn child_nodes(&self, parent_node_id: usize) -> Result<Vec<model::Node>> {
        let statement = self.conn.prepare_cached(
            r#"
            SELECT node_id, node_type, node_ns, node_name, node_value FROM nodes
                WHERE parent_node_id = ?1
                AND node_id != 0
                ORDER BY node_order
        "#,
        )?;

        statement
            .query_map([parent_node_id], |r| {
                Ok(model::RawNode {
                    node_id: r.get::<_, usize>(0)?,
                    node_type: NodeType::try_from(r.get::<_, u8>(1)?).unwrap(),
                    ns: r.get::<_, Option<String>>(2)?,
                    name: r.get::<_, Option<String>>(3)?,
                    value: r.get::<_, Option<String>>(4)?,
                }
                .into())
            })?
            .collect()
    }

    pub fn children(&self, parent_node_id: usize) -> Result<Vec<model::Element>> {
        let statement = self.conn.prepare_cached(
            r#"
            SELECT node_id, node_ns, node_name FROM nodes
                WHERE parent_node_id = ?1 AND node_type = ?2
                ORDER BY node_order
        "#,
        )?;

        statement
            .query_map([parent_node_id, NodeType::Element as usize], |r| {
                Ok(model::Element {
                    node_id: r.get::<_, usize>(0)?,
                    ns: r.get::<_, Option<String>>(1)?,
                    name: r.get::<_, String>(2)?,
                })
            })?
            .collect()
    }

    pub fn children_by_name(
        &self,
        parent_node_id: usize,
        element_name: &str,
    ) -> Result<Vec<model::Element>> {
        let element_name = if self.options.case_insensitive {
            Cow::Owned(element_name.to_lowercase())
        } else {
            Cow::Borrowed(element_name)
        };
        let statement = self.conn.prepare_cached(
            r#"
            SELECT node_id, node_ns, node_name FROM nodes
                WHERE parent_node_id = ?1 
                    AND node_type = ?2
                    AND node_name = ?3
                ORDER BY node_order
        "#,
        )?;

        statement
            .query_map(
                (parent_node_id, NodeType::Element as usize, element_name),
                |r| {
                    Ok(model::Element {
                        node_id: r.get::<_, usize>(0)?,
                        ns: r.get::<_, Option<String>>(1)?,
                        name: r.get::<_, String>(2)?,
                    })
                },
            )?
            .collect()
    }

    pub fn attr(&self, attr_id: usize) -> Result<model::Attr> {
        self.conn.query_row(
            r#"
                SELECT attr_ns, attr_name, attr_value FROM attrs WHERE attr_id = ?1 LIMIT 1
            "#,
            [attr_id],
            |r| {
                Ok(model::Attr {
                    attr_id,
                    ns: r.get::<_, Option<String>>(0)?,
                    name: r.get::<_, String>(1)?,
                    value: r.get::<_, String>(2)?,
                })
            },
        )
    }

    pub fn attr_by_name(
        &self,
        node_id: usize,
        attr_name: &str,
        attr_ns: Option<&str>,
    ) -> Result<Option<model::Attr>> {
        let attr_name = if self.options.case_insensitive {
            Cow::Owned(attr_name.to_lowercase())
        } else {
            Cow::Borrowed(attr_name)
        };

        if let Some(attr_ns) = attr_ns {
            self.conn
                .query_row(
                    r#"
                    SELECT attr_id, attr_value
                    FROM attrs WHERE parent_node_id = ?1 AND attr_name = ?2 AND attr_ns = ?3
                "#,
                    (node_id, &attr_name, attr_ns),
                    |r| {
                        Ok(model::Attr {
                            attr_id: r.get::<_, usize>(0)?,
                            ns: Some(attr_ns.to_string()),
                            name: attr_name.to_string(),
                            value: r.get::<_, String>(1)?,
                        })
                    },
                )
                .optional()
        } else {
            self.conn
                .query_row(
                    r#"
                    SELECT attr_id, attr_value
                    FROM attrs WHERE parent_node_id = ?1 AND attr_name = ?2 AND attr_ns IS NULL
                "#,
                    (node_id, &attr_name),
                    |r| {
                        Ok(model::Attr {
                            attr_id: r.get::<_, usize>(0)?,
                            ns: None,
                            name: attr_name.to_string(),
                            value: r.get::<_, String>(1)?,
                        })
                    },
                )
                .optional()
        }
    }

    pub fn attrs(&self, node_id: usize) -> Result<Vec<model::Attr>> {
        let statement = self.conn.prepare(
            r#"
                SELECT attr_id, attr_ns, attr_name, attr_value FROM attrs WHERE parent_node_id = ?1
            "#,
        )?;

        statement
            .query_map([node_id], |r| {
                Ok(model::Attr {
                    attr_id: r.get::<_, usize>(0)?,
                    ns: r.get::<_, Option<String>>(1)?,
                    name: r.get::<_, String>(2)?,
                    value: r.get::<_, String>(3)?,
                })
            })?
            .collect()
    }

    pub fn has_children(&self, node_id: usize) -> Result<bool> {
        let count = self.conn.query_row(
            r#"
                SELECT COUNT(*) FROM nodes WHERE parent_node_id = ?1 LIMIT 1
            "#,
            [node_id],
            |r| r.get::<_, usize>(0),
        )?;

        Ok(count > 0)
    }

    pub fn document_child_nodes(&self) -> Result<Vec<model::Node>> {
        self.child_nodes(0)
    }

    pub fn root(&self) -> Result<model::Element> {
        self.element(1)
    }

    pub fn descendent_nodes(&self, parent_node_id: usize) -> Result<Vec<model::Node>> {
        let stmt = self.conn.prepare_cached(
            r#"
            WITH RECURSIVE descendents(parent_id) AS (
                VALUES(?1)
                UNION
                SELECT node_id FROM nodes, descendents
                WHERE nodes.parent_node_id = descendents.parent_id
            )
            SELECT node_id, node_ns, node_name FROM nodes
            WHERE nodes.parent_node_id IN descendents
        "#,
        )?;

        stmt.query_map([parent_node_id], |r: &Row<'_>| {
            Ok(model::RawNode {
                node_id: r.get::<_, usize>(0)?,
                node_type: NodeType::try_from(r.get::<_, u8>(1)?).unwrap(),
                ns: r.get::<_, Option<String>>(2)?,
                name: r.get::<_, Option<String>>(3)?,
                value: r.get::<_, Option<String>>(4)?,
            }
            .into())
        })?
        .collect()
    }

    pub fn descendents(&self, parent_node_id: usize) -> Result<Vec<model::Element>> {
        let stmt = self.conn.prepare_cached(
            r#"
            WITH RECURSIVE descendents(parent_id) AS (
                VALUES(?1)
                UNION
                SELECT node_id FROM nodes, descendents
                WHERE nodes.parent_node_id = descendents.parent_id
            )
            SELECT node_id, node_ns, node_name FROM nodes
            WHERE nodes.node_type = 1 AND nodes.parent_node_id IN descendents
        "#,
        )?;

        stmt.query_map([parent_node_id], |r: &Row<'_>| {
            Ok(model::Element {
                node_id: r.get(0)?,
                ns: r.get(1)?,
                name: r.get(2)?,
            })
        })?
        .collect()
    }

    pub fn all_elements(&self) -> Result<Vec<model::Element>> {
        self.descendents(0)
    }

    pub fn all_nodes(&self) -> Result<Vec<model::Node>> {
        self.descendent_nodes(0)
    }

    pub fn inferred_type(&self, node_id: usize) -> Result<InferredType> {
        let statement = self.conn.prepare_cached(
            r#"
                SELECT inferred_type FROM nodes WHERE node_id = ?1 LIMIT 1
            "#,
        )?;
        let result = statement.query_row([node_id], |r| r.get::<_, String>(0))?;
        Ok(result.parse().unwrap())
    }

    pub fn node_raw_value(&self, node_id: usize) -> Result<Option<String>> {
        let statement = self.conn.prepare_cached(
            r#"
                SELECT node_value FROM nodes WHERE node_id = ?1 LIMIT 1
            "#,
        )?;
        let result = statement.query_row([node_id], |r| r.get::<_, Option<String>>(0))?;
        Ok(result)
    }

    pub fn attr_raw_value(&self, attr_id: usize) -> Result<Option<String>> {
        let statement = self.conn.prepare_cached(
            r#"
                SELECT attr_value FROM attrs WHERE attr_id = ?1 LIMIT 1
            "#,
        )?;
        let result = statement.query_row([attr_id], |r| r.get::<_, Option<String>>(0))?;
        Ok(result)
    }

    pub fn attr_inferred_type(&self, attr_id: usize) -> Result<InferredType> {
        let statement = self.conn.prepare_cached(
            r#"
                SELECT inferred_type FROM attrs WHERE attr_id = ?1 LIMIT 1
            "#,
        )?;
        let result = statement.query_row([attr_id], |r| r.get::<_, String>(0))?;
        Ok(result.parse().unwrap())
    }

    pub fn elements_matching_attr_value(
        &self,
        attr_name: &str,
        attr_value: &str,
    ) -> Result<Vec<model::Element>> {
        let attr_name = if self.options.case_insensitive {
            Cow::Owned(attr_name.to_lowercase())
        } else {
            Cow::Borrowed(attr_name)
        };
        let statement = self.conn.prepare(
            r#"
                SELECT n.node_id, n.node_ns, n.node_name, n.node_value FROM attrs a 
                JOIN nodes n ON n.node_id = a.parent_node_id
                WHERE a.attr_name = ?1 AND a.attr_value = ?2 AND n.node_type = 1
            "#,
        )?;

        statement
            .query_map((attr_name, attr_value), |r| {
                Ok(model::Element {
                    node_id: r.get::<_, usize>(0)?,
                    ns: r.get::<_, Option<String>>(1)?,
                    name: r.get::<_, String>(2)?,
                })
            })?
            .collect()
    }

    #[inline]
    pub fn to_string_pretty(&self) -> String {
        let mut s = vec![];
        self.print(&mut s, &Config::default_pretty(), &State::new(self, true))
            .unwrap();
        String::from_utf8(s).expect("invalid UTF-8")
    }

    #[inline]
    pub fn to_string_pretty_with_config(&self, config: &crate::writer::Config) -> String {
        let mut s = vec![];
        self.print(&mut s, config, &State::new(self, true)).unwrap();
        String::from_utf8(s).expect("invalid UTF-8")
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum NodeType {
    Document = 0,
    Element,
    Text,
    CData,
    Comment,
    Declaration,
    Doctype,
    ProcessingInstruction,
}

impl rusqlite::ToSql for NodeType {
    fn to_sql(&self) -> Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Owned(rusqlite::types::Value::Integer(
            *self as i64,
        )))
    }
}

impl TryFrom<u8> for NodeType {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(NodeType::Document),
            1 => Ok(NodeType::Element),
            2 => Ok(NodeType::Text),
            3 => Ok(NodeType::CData),
            4 => Ok(NodeType::Comment),
            5 => Ok(NodeType::Declaration),
            6 => Ok(NodeType::Doctype),
            7 => Ok(NodeType::ProcessingInstruction),
            _ => Err(()),
        }
    }
}

impl From<NodeType> for u8 {
    fn from(value: NodeType) -> Self {
        value as u8
    }
}
