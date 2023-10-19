use std::borrow::{Borrow, Cow};

use cssparser::{ParseError, ToCss};
use selectors::attr::{AttrSelectorOperation, CaseSensitivity, NamespaceConstraint};
use selectors::bloom::CountingBloomFilter;
use selectors::context::QuirksMode;
use selectors::parser::{
    NonTSPseudoClass, Parser, Selector as GenericSelector, SelectorImpl, SelectorList,
};
use selectors::parser::{PseudoElement, SelectorParseErrorKind};
use selectors::{self, matching, OpaqueElement};

use crate::document::DocumentDb;
use crate::model;

#[derive(Debug, Clone)]
pub struct Selectors;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Value(String);

impl cssparser::ToCss for Value {
    fn to_css<W>(&self, dest: &mut W) -> std::fmt::Result
    where
        W: std::fmt::Write,
    {
        write!(dest, "{}", self.0)
    }
}

impl From<&str> for Value {
    fn from(x: &str) -> Self {
        Value(x.to_string())
    }
}

impl AsRef<str> for Value {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Borrow<String> for Value {
    fn borrow(&self) -> &String {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PseudoClass {}

impl ToCss for PseudoClass {
    fn to_css<W>(&self, _dest: &mut W) -> std::fmt::Result
    where
        W: std::fmt::Write,
    {
        unimplemented!()
    }
}

impl NonTSPseudoClass for PseudoClass {
    type Impl = Selectors;

    fn is_active_or_hover(&self) -> bool {
        false
    }

    fn is_user_action_state(&self) -> bool {
        false
    }
}

impl PseudoElement for Value {
    type Impl = Selectors;
}

impl SelectorImpl for Selectors {
    type ExtraMatchingData = ();
    type AttrValue = Value;
    type Identifier = Value;
    type LocalName = Value;
    type NamespaceUrl = String;
    type NamespacePrefix = Value;
    type BorrowedNamespaceUrl = String;
    type BorrowedLocalName = String;
    type NonTSPseudoClass = PseudoClass;
    type PseudoElement = Value;
}

#[derive(Clone)]
pub struct ElementRef<'a> {
    element: Cow<'a, model::Element>,
    db: &'a DocumentDb,
}

impl std::fmt::Debug for ElementRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.element.node_id)
    }
}

impl selectors::Element for ElementRef<'_> {
    type Impl = Selectors;

    fn opaque(&self) -> OpaqueElement {
        OpaqueElement::new(&self.element.node_id)
    }

    fn parent_element(&self) -> Option<Self> {
        self.db
            .parent_element_id(self.element.node_id)
            .ok()
            .map(|node_id| Self {
                element: Cow::Owned(self.db.element(node_id).unwrap()),
                db: self.db,
            })
    }

    fn parent_node_is_shadow_root(&self) -> bool {
        false
    }

    fn containing_shadow_host(&self) -> Option<Self> {
        None
    }

    fn is_pseudo_element(&self) -> bool {
        false
    }

    fn prev_sibling_element(&self) -> Option<Self> {
        self.db
            .prev_sibling_element_id(self.element.node_id)
            .ok()
            .map(|node_id| Self {
                element: Cow::Owned(self.db.element(node_id).unwrap()),
                db: self.db,
            })
    }

    fn next_sibling_element(&self) -> Option<Self> {
        self.db
            .next_sibling_element_id(self.element.node_id)
            .ok()
            .map(|node_id| Self {
                element: Cow::Owned(self.db.element(node_id).unwrap()),
                db: self.db,
            })
    }

    fn is_html_element_in_html_document(&self) -> bool {
        false
    }

    fn has_local_name(&self, local_name: &<Self::Impl as SelectorImpl>::BorrowedLocalName) -> bool {
        &self.element.name == local_name
    }

    fn has_namespace(&self, ns: &<Self::Impl as SelectorImpl>::BorrowedNamespaceUrl) -> bool {
        self.element.ns.as_deref() == Some(ns)
    }

    fn is_same_type(&self, other: &Self) -> bool {
        self.element.name == other.element.name
    }

    fn attr_matches(
        &self,
        ns: &NamespaceConstraint<&<Self::Impl as SelectorImpl>::NamespaceUrl>,
        local_name: &<Self::Impl as SelectorImpl>::LocalName,
        operation: &AttrSelectorOperation<&<Self::Impl as SelectorImpl>::AttrValue>,
    ) -> bool {
        let ns = match ns {
            NamespaceConstraint::Any => None,
            NamespaceConstraint::Specific(ns) if ns == &"" => None,
            NamespaceConstraint::Specific(ns) => Some(&***ns),
        };

        let attr = self
            .db
            .attr_by_name(self.element.node_id, &local_name.0, ns)
            .unwrap();

        if let Some(val) = attr {
            operation.eval_str(&val.value)
        } else {
            false
        }
    }

    fn match_non_ts_pseudo_class<F>(
        &self,
        _pc: &<Self::Impl as SelectorImpl>::NonTSPseudoClass,
        _context: &mut selectors::context::MatchingContext<Self::Impl>,
        _flags_setter: &mut F,
    ) -> bool
    where
        F: FnMut(&Self, matching::ElementSelectorFlags),
    {
        false
    }

    fn match_pseudo_element(
        &self,
        _pe: &<Self::Impl as SelectorImpl>::PseudoElement,
        _context: &mut selectors::context::MatchingContext<Self::Impl>,
    ) -> bool {
        false
    }

    fn is_link(&self) -> bool {
        false
    }

    fn is_html_slot_element(&self) -> bool {
        false
    }

    fn has_id(
        &self,
        id: &<Self::Impl as SelectorImpl>::Identifier,
        case_sensitivity: CaseSensitivity,
    ) -> bool {
        let attr = self
            .db
            .attr_by_name(self.element.node_id, "id", None)
            .unwrap();

        match attr {
            Some(x) => case_sensitivity.eq(x.value.as_bytes(), id.0.as_bytes()),
            None => false,
        }
    }

    fn has_class(
        &self,
        name: &<Self::Impl as SelectorImpl>::Identifier,
        case_sensitivity: CaseSensitivity,
    ) -> bool {
        let attr = self
            .db
            .attr_by_name(self.element.node_id, "class", None)
            .unwrap();

        match attr {
            Some(x) => x
                .value
                .split_whitespace()
                .any(|x| case_sensitivity.eq(x.as_bytes(), name.0.as_bytes())),
            None => false,
        }
    }

    fn imported_part(
        &self,
        _name: &<Self::Impl as SelectorImpl>::Identifier,
    ) -> Option<<Self::Impl as SelectorImpl>::Identifier> {
        None
    }

    fn is_part(&self, _name: &<Self::Impl as SelectorImpl>::Identifier) -> bool {
        false
    }

    fn is_empty(&self) -> bool {
        self.db.has_children(self.element.node_id).unwrap()
    }

    fn is_root(&self) -> bool {
        self.element.node_id == 1
    }
}

struct TheParser;

impl<'i> Parser<'i> for TheParser {
    type Impl = Selectors;
    type Error = SelectorParseErrorKind<'i>;

    fn namespace_for_prefix(
        &self,
        prefix: &<Self::Impl as SelectorImpl>::NamespacePrefix,
    ) -> Option<<Self::Impl as SelectorImpl>::NamespaceUrl> {
        // This is a pretty nasty hack but gets the job done for now.
        Some(prefix.0.to_string())
    }
}

#[derive(Debug, Clone)]
struct SelectorInner(GenericSelector<Selectors>);

#[derive(Debug, Clone)]
pub struct Selector(Vec<SelectorInner>);

impl Selector {
    pub fn new(s: &str) -> Result<Selector, ParseError<SelectorParseErrorKind>> {
        let mut input = cssparser::ParserInput::new(s);
        match SelectorList::parse(&TheParser, &mut cssparser::Parser::new(&mut input)) {
            Ok(list) => Ok(Selector(list.0.into_iter().map(SelectorInner).collect())),
            Err(e) => Err(e),
        }
    }

    #[inline]
    pub fn match_one(&self, db: &DocumentDb) -> Result<Option<model::Element>, rusqlite::Error> {
        self.match_one_from(db, 0)
    }

    pub fn match_one_from(
        &self,
        db: &DocumentDb,
        node_id: usize,
    ) -> Result<Option<model::Element>, rusqlite::Error> {
        let bloom_filter = CountingBloomFilter::new();
        let mut context = matching::MatchingContext::new(
            matching::MatchingMode::Normal,
            Some(&bloom_filter),
            None,
            QuirksMode::NoQuirks,
        );

        let mut matched = None;

        // println!(
        //     "{:?}",
        //     self.0
        //         .iter()
        //         .map(|x| x.0.iter().collect::<Vec<_>>())
        //         .collect::<Vec<_>>()
        // );

        for element in db.descendents(node_id)? {
            let r = ElementRef {
                db,
                element: Cow::Borrowed(&element),
            };

            let x = self.0.iter().any(|s| {
                matching::matches_selector(&s.0, 0, None, &r, &mut context, &mut |_, _| {})
            });

            if x {
                matched = Some(element);
                break;
            }
        }

        Ok(matched)
    }

    #[inline]
    pub fn match_all(self, db: &DocumentDb) -> Result<Vec<model::Element>, rusqlite::Error> {
        self.match_all_from(db, 0)
    }

    pub fn match_all_from(
        self,
        db: &DocumentDb,
        node_id: usize,
    ) -> Result<Vec<model::Element>, rusqlite::Error> {
        let bloom_filter = CountingBloomFilter::new();
        let mut context = matching::MatchingContext::new(
            matching::MatchingMode::Normal,
            Some(&bloom_filter),
            None,
            QuirksMode::NoQuirks,
        );

        // println!(
        //     "{:?}",
        //     self.0
        //         .iter()
        //         .map(|x| x.0.iter_raw_match_order().collect::<Vec<_>>())
        //         .collect::<Vec<_>>()
        // );

        db.descendents(node_id)?
            .into_iter()
            .filter_map(|element| {
                let r = ElementRef {
                    db,
                    element: Cow::Borrowed(&element),
                };

                let x = self.0.iter().any(|s| {
                    // println!("{s:?}");
                    matching::matches_selector(&s.0, 0, None, &r, &mut context, &mut |_, _| {})
                });

                if x {
                    Some(Ok(element))
                } else {
                    None
                }
            })
            .collect::<Result<Vec<_>, _>>()
    }
}
