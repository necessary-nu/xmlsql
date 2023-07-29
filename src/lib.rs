pub mod model;
pub mod select;

use std::{
    fmt::Debug,
    io::BufRead,
    path::{Path, PathBuf},
};

use quick_xml::{
    events::{BytesStart, Event},
    reader::Reader,
};
use rusqlite::{types::ToSqlOutput, Batch, OpenFlags, OptionalExtension, Row};

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

#[derive(Debug)]
enum Mode {
    InMemory,
    OnDisk,
    TempDir(tempfile::TempDir),
}

#[derive(Debug)]
pub struct DocumentDb {
    conn: rusqlite::Connection,
    mode: Mode,
}

struct DocumentDbBuilder<'a> {
    conn: rusqlite::Transaction<'a>,
}

impl DocumentDbBuilder<'_> {
    fn commit(self) -> rusqlite::Result<()> {
        self.conn.commit()
    }

    pub fn insert_node(
        &self,
        parent_node_id: usize,
        node_type: NodeType,
        node_ns: Option<&str>,
        node_name: Option<&str>,
        node_value: Option<&str>,
        buffer_position: usize,
        node_order: usize,
    ) -> Result<usize, rusqlite::Error> {
        let node_id = self.conn.query_row(
            r#"
            INSERT INTO nodes(node_type, node_ns, node_name, node_value, buffer_position, parent_node_id, node_order)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            RETURNING node_id
        "#,
            (node_type, node_ns, node_name, node_value, buffer_position, parent_node_id, node_order),
            |r| r.get::<_, usize>(0),
        )?;

        Ok(node_id)
    }

    pub fn insert_attr(
        &self,
        parent_node_id: usize,
        attr_ns: Option<&str>,
        attr_name: &str,
        attr_value: &str,
        buffer_position: usize,
        attr_order: usize,
    ) -> Result<usize, rusqlite::Error> {
        self.conn.query_row(r#"
            INSERT INTO attrs(attr_ns, attr_name, attr_value, buffer_position, attr_order, parent_node_id)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            RETURNING attr_id
        "#, (
            attr_ns,
            attr_name,
            attr_value,
            buffer_position,
            attr_order,
            parent_node_id,
        ), |r| r.get::<_, usize>(0))
    }

    pub fn insert_root_element(
        &self,
        node_ns: Option<&str>,
        node_name: Option<&str>,
        buffer_position: usize,
        node_order: usize,
    ) -> Result<usize, rusqlite::Error> {
        self.conn.execute(
            r#"
            UPDATE nodes
                SET node_ns = ?1, node_name = ?2, buffer_position = ?3, node_order = ?4
                WHERE node_id = 1
        "#,
            (node_ns, node_name, buffer_position, node_order),
        )?;

        Ok(1)
    }
}

impl DocumentDb {
    fn create_in_memory() -> Result<Self, rusqlite::Error> {
        let conn = rusqlite::Connection::open_in_memory()?;
        Self::_create(conn, Mode::InMemory)
    }

    fn create_temp() -> Result<Self, rusqlite::Error> {
        let tmp = tempfile::tempdir()
            .map_err(|_| rusqlite::Error::InvalidPath(PathBuf::from(":temp:")))?;
        let conn = rusqlite::Connection::open(tmp.path())?;
        Self::_create(conn, Mode::TempDir(tmp))
    }

    fn create<P: AsRef<Path>>(path: P) -> Result<Self, rusqlite::Error> {
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

        Ok(Self { conn, mode })
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, rusqlite::Error> {
        let conn = rusqlite::Connection::open_with_flags(
            path.as_ref(),
            OpenFlags::SQLITE_OPEN_READ_WRITE,
        )?;
        Ok(Self {
            conn,
            mode: Mode::OnDisk,
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

        Ok(iter.collect::<Result<Vec<_>, _>>()?)
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
            }
            .into())
        })?;

        Ok(iter.collect::<Result<Vec<_>, _>>()?)
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

        Ok(iter.collect::<Result<Vec<_>, _>>()?)
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
        let mut rows = stmt.query_map([], |r: &Row<'_>| {
            Ok(model::Element {
                node_id: r.get(0)?,
                ns: r.get(1)?,
                name: r.get(2)?,
            })
        })?;

        while let Some(row) = rows.next() {
            if !callback(row?) {
                break;
            }
        }

        Ok(())
    }
}

fn parse_start_event<R>(
    reader: &quick_xml::Reader<R>,
    parser_state: &mut ParserState,
    db: &DocumentDbBuilder,
    event: &BytesStart<'_>,
) -> Result<(), Error> {
    let (local_name, prefix) = event.name().decompose();
    let local_name = std::str::from_utf8(local_name.as_ref())?;
    let prefix = match prefix.as_ref() {
        Some(x) => Some(std::str::from_utf8(x.as_ref())?),
        None => None,
    };

    let parent_node_id = parser_state.parent_node_id();

    let node_id = if matches!(parser_state.current(), ParserStateValue::InContext) {
        db.insert_root_element(
            prefix,
            Some(local_name),
            reader.buffer_position(),
            parser_state.current_order(),
        )?;
        parser_state.increment_order();
        parser_state.push(ParserStateValue::InRoot);
        1
    } else {
        let node_id = db.insert_node(
            parent_node_id,
            NodeType::Element,
            prefix,
            Some(local_name),
            None,
            reader.buffer_position(),
            parser_state.current_order(),
        )?;
        parser_state.increment_order();
        parser_state.push(ParserStateValue::InElement(node_id));
        node_id
    };

    for (n, x) in event.attributes().enumerate() {
        match x {
            Ok(x) => {
                let attr_ns = x.key.prefix();
                let attr_ns = match attr_ns.as_ref() {
                    Some(x) => Some(std::str::from_utf8(x.as_ref())?),
                    None => None,
                };

                let attr_name = x.key.local_name();
                let attr_name = std::str::from_utf8(attr_name.as_ref())?;
                let attr_value = &*x.decode_and_unescape_value(reader)?;

                db.insert_attr(
                    node_id,
                    attr_ns,
                    attr_name,
                    attr_value,
                    reader.buffer_position(),
                    n,
                )?;
            }
            Err(e) => return Err(quick_xml::Error::InvalidAttr(e).into()),
        }
    }

    Ok(())
}

#[derive(Debug, Clone, Copy)]
pub enum ParserStateValue {
    InContext,
    InRoot,
    InElement(usize),
}

#[derive(Debug, Default)]
pub struct ParserState {
    stack: Vec<ParserStateValue>,
    context_order: usize,
    order: Vec<usize>,
}

impl ParserState {
    pub fn current(&self) -> ParserStateValue {
        match self.stack.last() {
            Some(x) => *x,
            None => ParserStateValue::InContext,
        }
    }

    pub fn current_order(&self) -> usize {
        match self.order.last() {
            Some(v) => *v,
            None => self.context_order,
        }
    }

    pub fn increment_order(&mut self) {
        match self.order.last_mut() {
            Some(x) => *x += 1,
            None => self.context_order += 1,
        }
    }

    pub fn parent_node_id(&self) -> usize {
        match self.current() {
            ParserStateValue::InContext => 0,
            ParserStateValue::InRoot => 1,
            ParserStateValue::InElement(v) => v,
        }
    }

    pub fn push(&mut self, value: ParserStateValue) {
        self.stack.push(value);
        self.order.push(0);
    }

    pub fn pop(&mut self) {
        self.stack.pop();
        self.order.pop();
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Xml(#[from] quick_xml::Error),

    #[error("{0}")]
    Db(#[from] rusqlite::Error),

    #[error("{0}")]
    Utf8(#[from] std::str::Utf8Error),
}

#[derive(Debug, Default)]
pub struct ParseOptions {
    pub ignore_whitespace: bool,
}

pub fn parse_to_disk<R: BufRead, P: AsRef<Path>>(
    db_path: P,
    input: R,
    options: ParseOptions,
) -> Result<DocumentDb, Error> {
    let db = DocumentDb::create(db_path.as_ref())?;
    _parse(db, input, options)
}

pub fn parse_to_temp_file<R: BufRead>(
    input: R,
    options: ParseOptions,
) -> Result<DocumentDb, Error> {
    let db = DocumentDb::create_temp()?;
    _parse(db, input, options)
}

pub fn parse_in_memory<R: BufRead>(input: R, options: ParseOptions) -> Result<DocumentDb, Error> {
    let db = DocumentDb::create_in_memory()?;
    _parse(db, input, options)
}

fn _parse<R: BufRead>(
    mut doc_db: DocumentDb,
    input: R,
    options: ParseOptions,
) -> Result<DocumentDb, Error> {
    let mut reader = Reader::from_reader(input);
    reader.trim_text(options.ignore_whitespace);

    let mut parser_state = ParserState::default();

    let db = DocumentDbBuilder {
        conn: doc_db.conn.transaction()?,
    };

    let mut buf = Vec::new();
    loop {
        let event = reader.read_event_into(&mut buf)?;
        let parent_node_id = parser_state.parent_node_id();

        match event {
            Event::Start(event) => {
                parse_start_event(&reader, &mut parser_state, &db, &event)?;
            }
            Event::End(_) => {
                parser_state.pop();
            }
            Event::Empty(event) => {
                parse_start_event(&reader, &mut parser_state, &db, &event)?;
                parser_state.pop();
            }
            Event::Text(event) => {
                db.insert_node(
                    parent_node_id,
                    NodeType::Text,
                    None,
                    None,
                    Some(&event.unescape()?.as_ref()),
                    reader.buffer_position(),
                    parser_state.current_order(),
                )?;
                parser_state.increment_order();
            }
            Event::CData(event) => {
                db.insert_node(
                    parent_node_id,
                    NodeType::CData,
                    None,
                    None,
                    Some(std::str::from_utf8(&event.as_ref())?),
                    reader.buffer_position(),
                    parser_state.current_order(),
                )?;
                parser_state.increment_order();
            }
            Event::Comment(event) => {
                db.insert_node(
                    parent_node_id,
                    NodeType::Comment,
                    None,
                    None,
                    Some(&event.unescape()?.as_ref()),
                    reader.buffer_position(),
                    parser_state.current_order(),
                )?;
                parser_state.increment_order();
            }
            Event::Decl(event) => {
                db.insert_node(
                    parent_node_id,
                    NodeType::Declaration,
                    None,
                    None,
                    Some(std::str::from_utf8(&event.as_ref())?),
                    reader.buffer_position(),
                    parser_state.current_order(),
                )?;
                parser_state.increment_order();
            }
            Event::PI(event) => {
                db.insert_node(
                    parent_node_id,
                    NodeType::ProcessingInstruction,
                    None,
                    None,
                    Some(std::str::from_utf8(&event.as_ref())?),
                    reader.buffer_position(),
                    parser_state.current_order(),
                )?;
                parser_state.increment_order();
            }
            Event::DocType(event) => {
                db.insert_node(
                    parent_node_id,
                    NodeType::Doctype,
                    None,
                    None,
                    Some(std::str::from_utf8(&event.as_ref())?),
                    reader.buffer_position(),
                    parser_state.current_order(),
                )?;
                parser_state.increment_order();
            }
            Event::Eof => break,
        }

        buf.clear();
    }

    db.commit()?;

    Ok(doc_db)
}
