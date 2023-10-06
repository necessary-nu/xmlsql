use crate::{
    document::NodeType,
    infer::{infer_type, Inferred},
};

pub(crate) struct DocumentDbBuilder<'a> {
    pub(crate) conn: rusqlite::Transaction<'a>,
    pub(crate) infer_types: bool,
}

impl DocumentDbBuilder<'_> {
    #[inline(always)]
    pub fn commit(self) -> rusqlite::Result<()> {
        self.conn.commit()
    }

    #[inline(always)]
    #[allow(clippy::too_many_arguments)]
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
        let node_id = if self.infer_types {
            let inferred_type = if let Some(v) = node_value {
                infer_type(v)
            } else {
                Inferred::Empty
            };

            self.conn.query_row(
                r#"
                INSERT INTO nodes(node_type, node_ns, node_name, node_value, buffer_position, parent_node_id, node_order, inferred_type)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                RETURNING node_id
            "#,
                (node_type, node_ns, node_name, node_value, buffer_position, parent_node_id, node_order, inferred_type.as_type().as_str()),
                |r| r.get::<_, usize>(0),
            )?
        } else {
            self.conn.query_row(
                r#"
                INSERT INTO nodes(node_type, node_ns, node_name, node_value, buffer_position, parent_node_id, node_order)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                RETURNING node_id
            "#,
                (node_type, node_ns, node_name, node_value, buffer_position, parent_node_id, node_order),
                |r| r.get::<_, usize>(0),
            )?
        };

        Ok(node_id)
    }

    #[inline(always)]
    pub fn insert_attr(
        &self,
        parent_node_id: usize,
        attr_ns: Option<&str>,
        attr_name: &str,
        attr_value: &str,
        buffer_position: usize,
        attr_order: usize,
    ) -> Result<usize, rusqlite::Error> {
        if self.infer_types {
            let inferred_type = infer_type(attr_value);

            self.conn.query_row(r#"
                INSERT INTO attrs(attr_ns, attr_name, attr_value, buffer_position, attr_order, parent_node_id, inferred_type)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                RETURNING attr_id
            "#, (
                attr_ns,
                attr_name,
                attr_value,
                buffer_position,
                attr_order,
                parent_node_id,
                inferred_type.as_type().as_str(),
            ), |r| r.get::<_, usize>(0))
        } else {
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
    }

    #[inline(always)]
    pub fn insert_root_element(
        &self,
        node_ns: Option<&str>,
        node_name: Option<&str>,
        buffer_position: usize,
        node_order: usize,
    ) -> Result<(), rusqlite::Error> {
        self.conn.execute(
            r#"
            UPDATE nodes
                SET node_ns = ?1, node_name = ?2, buffer_position = ?3, node_order = ?4
                WHERE node_id = 1
        "#,
            (node_ns, node_name, buffer_position, node_order),
        )?;

        Ok(())
    }
}
