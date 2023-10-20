use xmlparser::{self, ElementEnd, Token};

use crate::{
    builder::{DocumentDbBuilder, InsertAttr, InsertNode, InsertRootElement},
    document::{DocumentDb, NodeType},
};

fn parse_start_event(
    local_name: &str,
    prefix: Option<&str>,
    position: usize,
    parser_state: &mut ParserState,
    tx: &mut crossbeam_channel::Sender<Message>,
    node_id_count: &mut usize,
) -> Result<(), Error> {
    let parent_node_id = parser_state.parent_node_id();

    if matches!(parser_state.current(), ParserStateValue::Document) {
        tx.send(Message::InsertRootElement(InsertRootElement::new(
            prefix.map(|x| x.to_owned()),
            Some(local_name.to_string()),
            position,
            parser_state.current_order(),
        )))?;
        parser_state.increment_order();
        parser_state.push(ParserStateValue::Root);
    } else {
        let node_id = *node_id_count;
        let node = InsertNode::new(
            node_id,
            parent_node_id,
            NodeType::Element,
            prefix.map(|x| x.to_string()),
            Some(local_name.to_string()),
            None,
            position,
            parser_state.current_order(),
        );
        tx.send(Message::InsertNode(node))?;

        *node_id_count += 1;

        parser_state.increment_order();
        parser_state.push(ParserStateValue::Element(node_id));
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
    #[inline(always)]
    pub fn current(&self) -> ParserStateValue {
        match self.stack.last() {
            Some(x) => *x,
            None => ParserStateValue::Document,
        }
    }

    #[inline(always)]
    pub fn current_order(&self) -> usize {
        match self.order.last() {
            Some(v) => *v,
            None => self.context_order,
        }
    }

    #[inline(always)]
    pub fn increment_order(&mut self) {
        match self.order.last_mut() {
            Some(x) => *x += 1,
            None => self.context_order += 1,
        }
    }

    #[inline(always)]
    pub fn parent_node_id(&self) -> usize {
        match self.current() {
            ParserStateValue::Document => 0,
            ParserStateValue::Root => 1,
            ParserStateValue::Element(v) => v,
        }
    }

    #[inline(always)]
    pub fn push(&mut self, value: ParserStateValue) {
        self.stack.push(value);
        self.order.push(0);
    }

    #[inline(always)]
    pub fn pop(&mut self) {
        self.stack.pop();
        self.order.pop();
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Xml(#[from] xmlparser::Error),

    #[error("{0}")]
    Db(#[from] rusqlite::Error),

    #[error("{0}")]
    Utf8(#[from] std::str::Utf8Error),

    #[error("{0}")]
    Channel(#[from] crossbeam_channel::SendError<Message>),
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ParseOptions {
    pub ignore_whitespace: bool,
    pub infer_types: bool,
    pub case_insensitive: bool,
}

pub enum Message {
    InsertNode(InsertNode),
    InsertAttr(InsertAttr),
    InsertRootElement(InsertRootElement),
}

fn mutate_text(text: &str, options: &ParseOptions) -> String {
    match (options.ignore_whitespace, options.case_insensitive) {
        (true, true) => text.trim().to_lowercase(),
        (true, false) => text.trim().to_string(),
        (false, true) => text.to_lowercase(),
        (false, false) => text.to_string(),
    }
}

pub(crate) fn parse(doc_db: DocumentDb, input: &str) -> Result<DocumentDb, Error> {
    let options = doc_db.options;

    let (mut tx, rx) = crossbeam_channel::bounded(1000000);

    let handle = std::thread::spawn(move || {
        let rx = rx;

        let mut doc_db = doc_db;
        let db = DocumentDbBuilder::new(doc_db.conn.transaction().unwrap(), options.infer_types);

        loop {
            let msg = match rx.recv() {
                Ok(msg) => msg,
                Err(_) => {
                    break;
                }
            };

            match msg {
                Message::InsertNode(msg) => {
                    db.insert_node(msg).map_err(|e| {
                        eprintln!("{e:?}");
                        e
                    })?;
                }
                Message::InsertAttr(msg) => {
                    db.insert_attr(msg).map_err(|e| {
                        eprintln!("{e:?}");
                        e
                    })?;
                }
                Message::InsertRootElement(msg) => {
                    db.insert_root_element(msg).map_err(|e| {
                        eprintln!("{e:?}");
                        e
                    })?;
                }
            }
        }

        db.add_indexes().unwrap();
        db.commit().unwrap();

        Ok::<_, Error>(doc_db)
    });

    let mut parser_state = ParserState::default();
    let mut node_id_count = 2usize;

    // let mut buf = Vec::new();
    for token in xmlparser::Tokenizer::from(input) {
        let token = token?;
        let parent_node_id = parser_state.parent_node_id();

        // println!("{:?}", token);

        match token {
            Token::Declaration { .. } => {
                tx.send(Message::InsertNode(InsertNode::new(
                    node_id_count,
                    parent_node_id,
                    NodeType::Declaration,
                    None,
                    None,
                    None, // TODO: merge them together
                    token.span().start(),
                    parser_state.current_order(),
                )))?;
                node_id_count += 1;
                parser_state.increment_order();
            }
            Token::ProcessingInstruction { .. } => {
                tx.send(Message::InsertNode(InsertNode::new(
                    node_id_count,
                    parent_node_id,
                    NodeType::ProcessingInstruction,
                    None,
                    None,
                    None, // TODO: merge them together
                    token.span().start(),
                    parser_state.current_order(),
                )))?;
                node_id_count += 1;
                parser_state.increment_order();
            }
            Token::Comment { text, .. } => {
                tx.send(Message::InsertNode(InsertNode::new(
                    node_id_count,
                    parent_node_id,
                    NodeType::Comment,
                    None,
                    None,
                    Some(if options.ignore_whitespace {
                        (&*text).trim().to_string()
                    } else {
                        text.to_string()
                    }),
                    token.span().start(),
                    parser_state.current_order(),
                )))?;
                node_id_count += 1;
                parser_state.increment_order();
            }
            Token::DtdStart { .. } => {
                tx.send(Message::InsertNode(InsertNode::new(
                    node_id_count,
                    parent_node_id,
                    NodeType::Doctype,
                    None,
                    None,
                    None, // TODO: merge them together
                    token.span().start(),
                    parser_state.current_order(),
                )))?;
                node_id_count += 1;
                parser_state.increment_order();
            }
            Token::EmptyDtd { .. } => {}
            Token::EntityDeclaration { .. } => {}
            Token::DtdEnd { .. } => {}
            Token::ElementStart {
                prefix,
                local,
                span,
            } => {
                let local = mutate_text(&*local, &options);
                let prefix = if !prefix.is_empty() {
                    Some(mutate_text(&*prefix, &options))
                } else {
                    None
                };
                parse_start_event(
                    &local,
                    prefix.as_deref(),
                    span.start(),
                    &mut parser_state,
                    &mut tx,
                    &mut node_id_count,
                )?;
            }
            Token::Attribute {
                prefix,
                local,
                value,
                span,
            } => {
                let prefix = if !prefix.is_empty() {
                    Some(mutate_text(&*prefix, &options))
                } else {
                    None
                };

                let local = if !local.is_empty() {
                    Some(mutate_text(&*local, &options))
                } else {
                    None
                };

                let value = if !value.is_empty() {
                    Some(&*value)
                } else {
                    None
                };

                tx.send(Message::InsertAttr(InsertAttr::new(
                    parent_node_id,
                    prefix,
                    local.unwrap_or_default(),
                    value.map(|x| x.to_string()).unwrap_or_default(),
                    span.start(),
                    parser_state.current_order(),
                )))?;
                parser_state.increment_order();
            }
            Token::ElementEnd { end, .. } => match end {
                ElementEnd::Open => continue,
                ElementEnd::Close(_, _) | ElementEnd::Empty => {
                    parser_state.pop();
                }
            },
            Token::Text { text } => {
                tx.send(Message::InsertNode(InsertNode::new(
                    node_id_count,
                    parent_node_id,
                    NodeType::Text,
                    None,
                    None,
                    Some(if options.ignore_whitespace {
                        (&*text).trim().to_string()
                    } else {
                        text.to_string()
                    }),
                    token.span().start(),
                    parser_state.current_order(),
                )))?;
                node_id_count += 1;
                parser_state.increment_order();
            }
            Token::Cdata { text, .. } => {
                tx.send(Message::InsertNode(InsertNode::new(
                    node_id_count,
                    parent_node_id,
                    NodeType::CData,
                    None,
                    None,
                    Some(if options.ignore_whitespace {
                        (&*text).trim().to_string()
                    } else {
                        text.to_string()
                    }),
                    token.span().start(),
                    parser_state.current_order(),
                )))?;
                node_id_count += 1;
                parser_state.increment_order();
            }
        }
    }

    drop(tx);

    let doc_db = handle.join().unwrap()?;

    Ok(doc_db)
}
