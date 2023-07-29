use std::path::{Path, PathBuf};

use rusqlite::{types::ToSqlOutput, Batch, OpenFlags, OptionalExtension, Row};

use crate::model;

#[derive(Debug)]
enum Mode {
    InMemory,
    OnDisk,
    TempDir(tempfile::TempDir),
}

#[derive(Debug)]
pub struct DocumentDb {
    pub(crate) conn: rusqlite::Connection,
    _mode: Mode,
}

impl DocumentDb {
    pub(crate) fn create_in_memory() -> Result<Self, rusqlite::Error> {
        let conn = rusqlite::Connection::open_in_memory()?;
        Self::_create(conn, Mode::InMemory)
    }

    pub(crate) fn create_temp() -> Result<Self, rusqlite::Error> {
        let tmp = tempfile::tempdir()
            .map_err(|_| rusqlite::Error::InvalidPath(PathBuf::from(":temp:")))?;
        let conn = rusqlite::Connection::open(tmp.path().join("db"))?;
        Self::_create(conn, Mode::TempDir(tmp))
    }

    pub(crate) fn create<P: AsRef<Path>>(path: P) -> Result<Self, rusqlite::Error> {
        let conn = rusqlite::Connection::open_with_flags(
            path.as_ref(),
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )?;
        Self::_create(conn, Mode::OnDisk)
    }

    fn _create(conn: rusqlite::Connection, mode: Mode) -> Result<Self, rusqlite::Error> {
        let mut batch = Batch::new(
            &conn,
            r#"
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
            "#,
        );

        while let Some(mut stmt) = batch.next()? {
            stmt.execute([])?;
        }

        Ok(Self { conn, _mode: mode })
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, rusqlite::Error> {
        let conn = rusqlite::Connection::open_with_flags(
            path.as_ref(),
            OpenFlags::SQLITE_OPEN_READ_WRITE,
        )?;
        Ok(Self {
            conn,
            _mode: Mode::OnDisk,
        })
    }

    pub fn parent_element_id(&self, node_id: usize) -> Result<usize, rusqlite::Error> {
        let node_id = self.conn.query_row(
            r#"
                SELECT parent_node_id FROM nodes WHERE node_id = ?1
            "#,
            [node_id],
            |r| r.get::<_, usize>(0),
        )?;
        Ok(node_id)
    }

    pub fn prev_sibling_element_id(&self, node_id: usize) -> Result<usize, rusqlite::Error> {
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

    pub fn next_sibling_element_id(&self, node_id: usize) -> Result<usize, rusqlite::Error> {
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

    pub fn element(&self, node_id: usize) -> Result<model::Element, rusqlite::Error> {
        self.conn.query_row(
            r#"
                SELECT node_ns, node_name FROM nodes WHERE node_id = ?1
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

    pub fn node(&self, node_id: usize) -> Result<model::Node, rusqlite::Error> {
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

    pub fn child_nodes(&self, parent_node_id: usize) -> Result<Vec<model::Node>, rusqlite::Error> {
        let mut statement = self.conn.prepare(
            r#"
            SELECT node_id, node_type, node_ns, node_name, node_value FROM nodes
                WHERE parent_node_id = ?1
                ORDER BY node_order
        "#,
        )?;

        let iter = statement.query_map([parent_node_id], |r| {
            Ok(model::RawNode {
                node_id: r.get::<_, usize>(0)?,
                node_type: NodeType::try_from(r.get::<_, u8>(1)?).unwrap(),
                ns: r.get::<_, Option<String>>(2)?,
                name: r.get::<_, Option<String>>(3)?,
                value: r.get::<_, Option<String>>(4)?,
            }
            .into())
        })?;

        iter.collect::<Result<Vec<_>, _>>()
    }

    pub fn children(&self, parent_node_id: usize) -> Result<Vec<model::Element>, rusqlite::Error> {
        let mut statement = self.conn.prepare(
            r#"
            SELECT node_id, node_ns, node_name FROM nodes
                WHERE parent_node_id = ?1 AND node_type = ?2
                ORDER BY node_order
        "#,
        )?;

        let iter = statement.query_map([parent_node_id, NodeType::Element as usize], |r| {
            Ok(model::Element {
                node_id: r.get::<_, usize>(0)?,
                ns: r.get::<_, Option<String>>(1)?,
                name: r.get::<_, String>(2)?,
            })
        })?;

        iter.collect::<Result<Vec<_>, _>>()
    }

    pub fn attr(&self, attr_id: usize) -> Result<model::Attr, rusqlite::Error> {
        self.conn.query_row(
            r#"
                SELECT attr_ns, attr_name, attr_value FROM attrs WHERE attr_id = ?1
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
    ) -> Result<Option<model::Attr>, rusqlite::Error> {
        if let Some(attr_ns) = attr_ns {
            self.conn
                .query_row(
                    r#"
                    SELECT attr_id, attr_value
                    FROM attrs WHERE parent_node_id = ?1 AND attr_name = ?2 AND attr_ns = ?3
                "#,
                    (node_id, attr_name, attr_ns),
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
                    (node_id, attr_name),
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

    pub fn attrs(&self, node_id: usize) -> Result<Vec<model::Attr>, rusqlite::Error> {
        let mut statement = self.conn.prepare(
            r#"
                SELECT attr_id, attr_ns, attr_name, attr_value FROM attrs WHERE parent_node_id = ?1
            "#,
        )?;

        let iter = statement.query_map([node_id], |r| {
            Ok(model::Attr {
                attr_id: r.get::<_, usize>(0)?,
                ns: r.get::<_, Option<String>>(1)?,
                name: r.get::<_, String>(2)?,
                value: r.get::<_, String>(3)?,
            })
        })?;

        iter.collect::<Result<Vec<_>, _>>()
    }

    pub fn has_children(&self, node_id: usize) -> Result<bool, rusqlite::Error> {
        let count = self.conn.query_row(
            r#"
                SELECT COUNT(*) FROM nodes WHERE parent_node_id = ?1
            "#,
            [node_id],
            |r| r.get::<_, usize>(0),
        )?;

        Ok(count > 0)
    }

    pub fn document_child_nodes(&self) -> Result<Vec<model::Node>, rusqlite::Error> {
        self.child_nodes(0)
    }

    pub fn root(&self) -> Result<model::Element, rusqlite::Error> {
        self.element(1)
    }

    pub fn all_elements<F: FnMut(model::Element) -> bool>(
        &self,
        mut callback: F,
    ) -> Result<(), rusqlite::Error> {
        // TODO: make this into a chunked iterator to save memory.
        let mut stmt = self
            .conn
            .prepare("SELECT node_id, node_ns, node_name FROM nodes WHERE node_type = 1")?;
        let rows = stmt.query_map([], |r: &Row<'_>| {
            Ok(model::Element {
                node_id: r.get(0)?,
                ns: r.get(1)?,
                name: r.get(2)?,
            })
        })?;

        for row in rows {
            if !callback(row?) {
                break;
            }
        }

        Ok(())
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
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
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