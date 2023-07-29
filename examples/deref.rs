use std::io::{BufReader, Cursor};

use xmlsql::{select::Selector, ParseOptions};

fn main() {
    let db = xmlsql::parse_in_memory(
        BufReader::new(Cursor::new(include_str!("./ex2.xml"))),
        ParseOptions {
            ignore_whitespace: true,
        },
    )
    .unwrap();

    let sel = Selector::new("ref[to]").unwrap();
    let refs = sel.match_all(&db);

    println!("{:?}", &refs);
    let targets = refs
        .into_iter()
        .filter_map(|x| db.attr_by_name(x.node_id, "to", None).ok().flatten())
        .filter_map(|x| {
            Selector::new(&format!("[id='{}']", x.value))
                .unwrap()
                .match_one(&db)
        })
        .collect::<Vec<_>>();

    println!("{:?}", targets);
}