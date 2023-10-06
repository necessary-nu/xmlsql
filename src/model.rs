use crate::NodeType;

#[derive(Debug, Clone)]
pub struct Element {
    pub node_id: usize,
    pub ns: Option<String>,
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct Text {
    pub node_id: usize,
    pub value: String,
}

#[derive(Debug, Clone)]
pub struct Comment {
    pub node_id: usize,
    pub value: String,
}

#[derive(Debug, Clone)]
pub struct CData {
    pub node_id: usize,
    pub value: String,
}

#[derive(Debug, Clone)]
pub struct Attr {
    pub attr_id: usize,
    pub ns: Option<String>,
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone)]
pub struct RawNode {
    pub node_id: usize,
    pub node_type: NodeType,
    pub ns: Option<String>,
    pub name: Option<String>,
    pub value: Option<String>,
}

impl From<RawNode> for Node {
    fn from(value: RawNode) -> Self {
        let RawNode {
            node_id,
            node_type,
            ns,
            name,
            value,
        } = value;

        match node_type {
            NodeType::Element => Node::Element(Element {
                node_id,
                ns,
                name: name.unwrap_or_default(),
            }),
            NodeType::Text => Node::Text(Text {
                node_id,
                value: value.unwrap_or_default(),
            }),
            NodeType::CData => Node::CData(CData {
                node_id,
                value: value.unwrap_or_default(),
            }),
            NodeType::Comment => Node::Comment(Comment {
                node_id,
                value: value.unwrap_or_default(),
            }),
            NodeType::Declaration => Node::Declaration(Declaration {
                node_id,
                value: value.unwrap_or_default(),
            }),
            NodeType::Doctype => Node::Doctype(Doctype {
                node_id,
                value: value.unwrap_or_default(),
            }),
            NodeType::ProcessingInstruction => Node::ProcessingInstruction(ProcessingInstruction {
                node_id,
                value: value.unwrap_or_default(),
            }),
            NodeType::Document => panic!("GlobalContext is not a supported node type"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Declaration {
    pub node_id: usize,
    pub value: String,
}

#[derive(Debug, Clone)]
pub struct Doctype {
    pub node_id: usize,
    pub value: String,
}

#[derive(Debug, Clone)]
pub struct ProcessingInstruction {
    pub node_id: usize,
    pub value: String,
}

#[derive(Debug, Clone)]
pub enum Node {
    Element(Element),
    Text(Text),
    Comment(Comment),
    CData(CData),
    Declaration(Declaration),
    Doctype(Doctype),
    ProcessingInstruction(ProcessingInstruction),
}

impl Node {
    pub fn node_id(&self) -> usize {
        match self {
            Node::Element(x) => x.node_id,
            Node::Text(x) => x.node_id,
            Node::Comment(x) => x.node_id,
            Node::CData(x) => x.node_id,
            Node::Declaration(x) => x.node_id,
            Node::Doctype(x) => x.node_id,
            Node::ProcessingInstruction(x) => x.node_id,
        }
    }
}
