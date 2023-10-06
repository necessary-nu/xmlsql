use std::{
    borrow::Cow,
    collections::BTreeMap,
    fmt::Display,
    io::{self, Write},
    str,
};

use crate::{
    model::{Attr, Declaration, Element, Node},
    DocumentDb,
};

// use indexmap::IndexMap;
// use qname::QName;
use unic_ucd::GeneralCategory;

// use crate::{
//     document::{Declaration, Document},
//     key::DocKey,
//     value::{ElementValue, NodeValue},
//     Node,
// };

pub trait Print<Config, Context = ()> {
    fn print(&self, f: &mut dyn Write, config: &Config, context: &Context) -> std::io::Result<()>;
}

#[derive(Debug, Clone, Default)]
pub struct Config {
    pub is_pretty: bool,
    pub indent: usize,
    pub end_pad: usize,
    pub max_line_length: usize,
    pub entity_mode: EntityMode,
    pub indent_text_nodes: bool,
}

impl Config {
    pub fn default_pretty() -> Self {
        Config {
            is_pretty: true,
            indent: 2,
            end_pad: 1,
            max_line_length: 120,
            entity_mode: EntityMode::Standard,
            indent_text_nodes: true,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct State<'a> {
    pub is_pretty: bool,
    pub indent: usize,
    pub node_id: usize,
    pub doc: &'a DocumentDb,
}

impl<'a> State<'a> {
    pub(crate) fn new(document: &'a DocumentDb, is_pretty: bool) -> Self {
        Self {
            is_pretty,
            indent: 0,
            doc: document,
            node_id: 0,
        }
    }

    fn with_indent(&self, config: &Config) -> Self {
        if !config.is_pretty {
            return self.clone();
        }

        State {
            is_pretty: self.is_pretty,
            indent: self.indent + config.indent,
            node_id: self.node_id,
            doc: self.doc,
        }
    }

    fn without_pretty(&self) -> Self {
        State {
            is_pretty: false,
            indent: 0,
            node_id: self.node_id,
            doc: self.doc,
        }
    }

    fn with_node_id(&self, node_id: usize) -> Self {
        State {
            is_pretty: self.is_pretty,
            indent: self.indent,
            node_id,
            doc: self.doc,
        }
    }
}

impl Print<Config, State<'_>> for Declaration {
    fn print(
        &self,
        f: &mut dyn Write,
        _config: &Config,
        context: &State<'_>,
    ) -> std::io::Result<()> {
        write!(f, "<?xml")?;
        write!(f, "{}", self.value)?;
        write!(f, "?>")?;

        if context.is_pretty {
            writeln!(f)?;
        }

        Ok(())
    }
}

impl Display for DocumentDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut config = if f.alternate() {
            Config::default_pretty()
        } else {
            Config::default()
        };

        if let Some(width) = f.width() {
            config.is_pretty = true;
            config.indent = width;
        }

        if let Some(precision) = f.precision() {
            config.is_pretty = true;
            config.max_line_length = precision;
        }

        self.print(
            &mut FmtWriter(f),
            &config,
            &State::new(self, config.is_pretty),
        )
        .map_err(|_| std::fmt::Error)
    }
}

impl Print<Config, State<'_>> for DocumentDb {
    fn print(
        &self,
        f: &mut dyn Write,
        config: &Config,
        context: &State<'_>,
    ) -> std::io::Result<()> {
        // if let Some(decl) = self.decl.as_ref() {
        //     Print::print(decl, f, config, context)?;
        // }
        for node in self.child_nodes(0).unwrap() {
            let node = node.unwrap();
            node.print(f, config, &context.with_node_id(node.node_id()))?;
        }

        Ok(())
    }
}

fn fmt_attrs<'a>(
    f: &mut dyn Write,
    tag: &str,
    config: &Config,
    context: &State,
    attrs: &[Attr],
) -> io::Result<()> {
    let line_length = tag.len()
        + 2
        + attrs
            .iter()
            .fold(0usize, |acc, x| acc + x.name.len() + x.value.len() + 4);

    let is_newlines = context.is_pretty && line_length > config.max_line_length;
    let context = context.with_indent(config);

    let mut iter = attrs.iter();

    if let Some(x) = iter.next() {
        write!(
            f,
            "{}=\"{}\"",
            x.name,
            process_entities(&x.value, config.entity_mode, false, false)
        )?;
    }

    if let Some(x) = iter.next() {
        if is_newlines {
            writeln!(f)?;
            write!(f, "{:>indent$}", "", indent = context.indent)?;
        } else {
            write!(f, " ")?;
        }
        write!(
            f,
            "{}=\"{}\"",
            x.name,
            process_entities(&x.value, config.entity_mode, false, false)
        )?;
    } else {
        return Ok(());
    }

    for x in iter {
        if is_newlines {
            writeln!(f)?;
            write!(f, "{:>indent$}", "", indent = context.indent)?;
        } else {
            write!(f, " ")?;
        }
        write!(
            f,
            "{}=\"{}\"",
            x.name,
            process_entities(&x.value, config.entity_mode, false, false)
        )?;
    }

    Ok(())
}

impl Print<Config, State<'_>> for Element {
    fn print(
        &self,
        f: &mut dyn Write,
        config: &Config,
        context: &State<'_>,
    ) -> std::io::Result<()> {
        let nodes = context
            .doc
            .child_nodes(self.node_id)
            .unwrap()
            .flat_map(Result::ok)
            .collect::<Vec<_>>();

        if nodes.is_empty() {
            let attrs = context
                .doc
                .attrs(self.node_id)
                .unwrap()
                .flat_map(Result::ok)
                .collect::<Vec<_>>();
            if !attrs.is_empty() {
                write!(f, "{:>indent$}<{}", "", self.name, indent = context.indent)?;
                let line_length = &self.name.len()
                    + 2
                    + attrs.iter().take(1).fold(0usize, |acc, attr| {
                        acc + attr.name.len() + attr.value.len() + 4
                    });
                let is_newlines = context.is_pretty && line_length > config.max_line_length;
                if is_newlines {
                    writeln!(f)?;
                    write!(
                        f,
                        "{:>indent$}",
                        "",
                        indent = context.indent + config.indent
                    )?;
                } else {
                    write!(f, " ")?;
                }
                fmt_attrs(f, &self.name, config, context, &attrs)?;
                write!(f, "{:>end_pad$}/>", "", end_pad = config.end_pad)?;
                if context.is_pretty {
                    writeln!(f)?;
                }
                return Ok(());
            } else {
                write!(
                    f,
                    "{:>indent$}<{:>end_pad$}/>",
                    "",
                    self.name,
                    indent = context.indent,
                    end_pad = config.end_pad
                )?;
                if context.is_pretty {
                    writeln!(f)?;
                }
                return Ok(());
            }
        }

        let has_text = nodes
            .iter()
            .any(|x| matches!(x, Node::Text(_) | Node::CData(_)));

        let attrs = context
            .doc
            .attrs(self.node_id)
            .unwrap()
            .flat_map(Result::ok)
            .collect::<Vec<_>>();
        if !attrs.is_empty() {
            write!(f, "{:>indent$}<{}", "", self.name, indent = context.indent)?;
            let line_length = &self.name.len()
                + 2
                + attrs.iter().take(1).fold(0usize, |acc, attr| {
                    acc + attr.name.len() + attr.value.len() + 4
                });
            let is_newlines = context.is_pretty && line_length > config.max_line_length;
            if is_newlines {
                writeln!(f)?;
                write!(
                    f,
                    "{:>indent$}",
                    "",
                    indent = context.indent + config.indent
                )?;
            } else {
                write!(f, " ")?;
            }
            fmt_attrs(f, &self.name, config, context, &attrs)?;
            write!(f, ">")?;
            if (config.indent_text_nodes || !has_text) && context.is_pretty {
                writeln!(f)?;
            }
        } else {
            write!(f, "{:>indent$}<{}>", "", self.name, indent = context.indent)?;
            if (config.indent_text_nodes || !has_text) && context.is_pretty {
                writeln!(f)?;
            }
        }

        let child_context = {
            if has_text && !config.indent_text_nodes {
                context.without_pretty()
            } else {
                context.with_indent(config)
            }
        };

        for child in nodes.iter() {
            child.print(f, config, &child_context.with_node_id(child.node_id()))?;
        }

        if (config.indent_text_nodes || !has_text) && context.is_pretty {
            write!(
                f,
                "{:>indent$}</{}>",
                "",
                self.name,
                indent = context.indent
            )?;

            writeln!(f)?;
        } else {
            write!(f, "</{}>", self.name)?;
            if context.is_pretty {
                writeln!(f)?;
            }
        }

        Ok(())
    }
}

impl Print<Config, State<'_>> for Node {
    fn print(
        &self,
        f: &mut dyn Write,
        config: &Config,
        context: &State<'_>,
    ) -> std::io::Result<()> {
        if let Node::Element(e) = self {
            return e.print(f, config, context);
        }

        if let Node::Text(t) = self {
            if config.indent_text_nodes && context.is_pretty {
                write!(f, "{:>indent$}", "", indent = context.indent)?;
            }

            write!(
                f,
                "{}",
                &*process_entities(&t.value, config.entity_mode, true, true)
            )?;

            if config.indent_text_nodes && context.is_pretty {
                writeln!(f)?;
            }

            return Ok(());
        }

        if let Node::CData(t) = self {
            if config.indent_text_nodes && context.is_pretty {
                write!(f, "{:>indent$}", "", indent = context.indent)?;
            }

            write!(f, "<![CDATA[{}]]>", t.value)?;

            if config.indent_text_nodes && context.is_pretty {
                writeln!(f)?;
            }

            return Ok(());
        }

        if context.is_pretty {
            write!(f, "{:>indent$}", "", indent = context.indent)?;
        }

        match self {
            Node::ProcessingInstruction(t) => write!(f, "<!{}>", t.value),
            Node::Comment(t) => write!(
                f,
                "<!--{}-->",
                process_entities(&t.value, config.entity_mode, true, true)
            ),
            Node::Declaration(d) => d.print(f, config, context),
            x => {
                panic!("What? {:?}", x)
            }
        }?;

        if context.is_pretty {
            writeln!(f)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntityMode {
    Standard,
    Hex,
}

impl Default for EntityMode {
    fn default() -> Self {
        Self::Standard
    }
}

fn process_entities(
    input: &str,
    mode: EntityMode,
    allow_separators: bool,
    is_text: bool,
) -> Cow<'_, str> {
    if input.chars().any(|ch| {
        if ['<', '>', '\'', '"', '&'].contains(&ch) || ch.is_ascii_control() {
            return true;
        }
        let cat = GeneralCategory::of(ch);
        cat.is_separator() || cat.is_other()
    }) {
        let mut s = String::with_capacity(input.len());
        input.chars().for_each(|ch| {
            s.push_str(match (mode, ch) {
                (EntityMode::Standard, '&') => "&amp;",
                (EntityMode::Standard, '\'') if !is_text => "&apos;",
                (EntityMode::Standard, '"') if !is_text => "&quot;",
                (EntityMode::Standard, '<') => "&lt;",
                (EntityMode::Standard, '>') => "&gt;",
                (EntityMode::Hex, '&' | '\'' | '"' | '<' | '>') => {
                    s.push_str(&format!("&#x{:>04X};", ch as u32));
                    return;
                }
                (_, ch) if !ch.is_ascii_whitespace() && ch.is_ascii_control() => {
                    s.push_str(&format!("&#x{:>04X};", ch as u32));
                    return;
                }
                (_, other) => {
                    let cat = GeneralCategory::of(other);

                    let is_ws = ch != '\u{00a0}'
                        && (ch == ' '
                            || allow_separators
                                && (other.is_ascii_whitespace() || cat.is_separator()));
                    let is_printable = !(cat.is_separator() || cat.is_other());

                    if is_ws || is_printable {
                        s.push(other);
                    } else {
                        s.push_str(&format!("&#x{:>04X};", ch as u32));
                    }
                    return;
                }
            })
        });
        Cow::Owned(s)
    } else {
        Cow::Borrowed(input)
    }
}

struct FmtWriter<'a, 'b>(&'b mut std::fmt::Formatter<'a>);

impl Write for FmtWriter<'_, '_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let s = std::str::from_utf8(buf).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.0
            .write_str(s)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(s.as_bytes().len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
