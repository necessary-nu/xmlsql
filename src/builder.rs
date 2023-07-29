use crate::document::NodeType;

pub(crate) struct DocumentDbBuilder<'a> {
    pub(crate) conn: rusqlite::Transaction<'a>,
}

impl DocumentDbBuilder<'_> {
    pub fn commit(self) -> rusqlite::Result<()> {
        self.conn.commit()
    }

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
