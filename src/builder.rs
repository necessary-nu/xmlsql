use crate::{
    document::NodeType,
    infer::{infer_type, Inferred},
};

const INDEXES: &str = r#"
CREATE INDEX IF NOT EXISTS idx_nodes_parent_node_id ON nodes(parent_node_id);
CREATE INDEX IF NOT EXISTS idx_attrs_parent_node_id ON attrs(parent_node_id);
CREATE INDEX IF NOT EXISTS idx_nodes_name ON nodes(node_name);
CREATE INDEX IF NOT EXISTS idx_nodes_type_elements ON nodes(node_type) WHERE node_type = 1;
CREATE INDEX IF NOT EXISTS idx_nodes_descendents ON nodes(node_type, parent_node_id);
CREATE INDEX IF NOT EXISTS idx_attrs_name ON attrs(attr_name);
"#;

pub(crate) struct DocumentDbBuilder<'a> {
    pub(crate) conn: rusqlite::Transaction<'a>,
    pub(crate) infer_types: bool,
}

#[derive(Debug)]
pub struct InsertNode {
    node_id: usize,
    parent_node_id: usize,
    node_type: NodeType,
    node_ns: Option<String>,
    node_name: Option<String>,
    node_value: Option<String>,
    buffer_position: usize,
    node_order: usize,
}

impl InsertNode {
    pub fn new(
        node_id: usize,
        parent_node_id: usize,
        node_type: NodeType,
        node_ns: Option<String>,
        node_name: Option<String>,
        node_value: Option<String>,
        buffer_position: usize,
        node_order: usize,
    ) -> Self {
        Self {
            node_id,
            parent_node_id,
            node_type,
            node_ns,
            node_name,
            node_value,
            buffer_position,
            node_order,
        }
    }
}

pub struct InsertAttr {
    parent_node_id: usize,
    attr_ns: Option<String>,
    attr_name: String,
    attr_value: String,
    buffer_position: usize,
    attr_order: usize,
}

impl InsertAttr {
    pub fn new(
        parent_node_id: usize,
        attr_ns: Option<String>,
        attr_name: String,
        attr_value: String,
        buffer_position: usize,
        attr_order: usize,
    ) -> Self {
        Self {
            parent_node_id,
            attr_ns,
            attr_name,
            attr_value,
            buffer_position,
            attr_order,
        }
    }
}

pub struct InsertRootElement {
    node_ns: Option<String>,
    node_name: Option<String>,
    buffer_position: usize,
    node_order: usize,
}

impl InsertRootElement {
    pub fn new(
        node_ns: Option<String>,
        node_name: Option<String>,
        buffer_position: usize,
        node_order: usize,
    ) -> Self {
        Self {
            node_ns,
            node_name,
            buffer_position,
            node_order,
        }
    }
}

impl<'a> DocumentDbBuilder<'a> {
    pub fn new(conn: rusqlite::Transaction<'a>, infer_types: bool) -> Self {
        Self { conn, infer_types }
    }

    #[inline(always)]
    pub fn commit(self) -> rusqlite::Result<()> {
        self.conn.commit()
    }

    #[inline(always)]
    pub fn add_indexes(&self) -> rusqlite::Result<()> {
        self.conn.execute_batch(INDEXES)?;
        Ok(())
    }

    #[inline(always)]
    #[allow(clippy::too_many_arguments)]
    pub fn insert_node(&self, data: InsertNode) -> Result<(), rusqlite::Error> {
        let InsertNode {
            node_id,
            parent_node_id,
            node_type,
            node_ns,
            node_name,
            node_value,
            buffer_position,
            node_order,
        } = data;

        // return Ok(0);
        if self.infer_types {
            let inferred_type = if let Some(v) = &node_value {
                infer_type(&v)
            } else {
                Inferred::Empty
            };

            let mut stmt = self.conn.prepare_cached(
                r#"
                INSERT INTO nodes(node_id, node_type, node_ns, node_name, node_value, buffer_position, parent_node_id, node_order, inferred_type)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            "#)?;

            stmt.execute((
                node_id,
                node_type,
                node_ns,
                node_name,
                node_value,
                buffer_position,
                parent_node_id,
                node_order,
                inferred_type.as_type().as_str(),
            ))?;
        } else {
            let mut stmt = self.conn.prepare_cached(
                r#"
                INSERT INTO nodes(node_id, node_type, node_ns, node_name, node_value, buffer_position, parent_node_id, node_order)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            "#)?;

            stmt.execute((
                node_id,
                node_type,
                node_ns,
                node_name,
                node_value,
                buffer_position,
                parent_node_id,
                node_order,
            ))?;
        };

        Ok(())
    }

    #[inline(always)]
    pub fn insert_attr(&self, data: InsertAttr) -> Result<(), rusqlite::Error> {
        let InsertAttr {
            parent_node_id,
            attr_ns,
            attr_name,
            attr_value,
            buffer_position,
            attr_order,
        } = data;
        // return Ok(0);
        if self.infer_types {
            let inferred_type = infer_type(&attr_value);

            let mut stmt = self.conn.prepare_cached(r#"
                INSERT INTO attrs(attr_ns, attr_name, attr_value, buffer_position, attr_order, parent_node_id, inferred_type)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            "#)?;

            stmt.execute((
                attr_ns,
                attr_name,
                attr_value,
                buffer_position,
                attr_order,
                parent_node_id,
                inferred_type.as_type().as_str(),
            ))?;
        } else {
            let mut stmt = self.conn.prepare_cached(r#"
                INSERT INTO attrs(attr_ns, attr_name, attr_value, buffer_position, attr_order, parent_node_id)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            "#)?;

            stmt.execute((
                attr_ns,
                attr_name,
                attr_value,
                buffer_position,
                attr_order,
                parent_node_id,
            ))?;
        }

        Ok(())
    }

    #[inline(always)]
    pub fn insert_root_element(&self, data: InsertRootElement) -> Result<(), rusqlite::Error> {
        let InsertRootElement {
            node_ns,
            node_name,
            buffer_position,
            node_order,
        } = data;

        // return Ok(());
        let mut stmt = self.conn.prepare_cached(
            r#"
            UPDATE nodes
                SET node_ns = ?1, node_name = ?2, buffer_position = ?3, node_order = ?4
                WHERE node_id = 1
        "#,
        )?;

        stmt.execute((node_ns, node_name, buffer_position, node_order))?;

        Ok(())
    }
}
