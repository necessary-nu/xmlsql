use std::io::BufRead;

use quick_xml::{
    events::{BytesStart, Event},
    Reader,
};

use crate::{
    builder::DocumentDbBuilder,
    document::{DocumentDb, NodeType},
};

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

    let node_id = if matches!(parser_state.current(), ParserStateValue::Document) {
        db.insert_root_element(
            prefix,
            Some(local_name),
            reader.buffer_position(),
            parser_state.current_order(),
        )?;
        parser_state.increment_order();
        parser_state.push(ParserStateValue::Root);
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
        parser_state.push(ParserStateValue::Element(node_id));
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
    Document,
    Root,
    Element(usize),
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
            None => ParserStateValue::Document,
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
            ParserStateValue::Document => 0,
            ParserStateValue::Root => 1,
            ParserStateValue::Element(v) => v,
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
    pub infer_types: bool,
}

pub(crate) fn parse<R: BufRead>(
    mut doc_db: DocumentDb,
    input: R,
    options: ParseOptions,
) -> Result<DocumentDb, Error> {
    let mut reader = Reader::from_reader(input);
    reader.trim_text(options.ignore_whitespace);

    let mut parser_state = ParserState::default();

    let db = DocumentDbBuilder {
        conn: doc_db.conn.transaction()?,
        infer_types: options.infer_types,
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
                    Some(event.unescape()?.as_ref()),
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
                    Some(std::str::from_utf8(event.as_ref())?),
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
                    Some(event.unescape()?.as_ref()),
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
                    Some(std::str::from_utf8(event.as_ref())?),
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
                    Some(std::str::from_utf8(event.as_ref())?),
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
                    Some(std::str::from_utf8(event.as_ref())?),
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
