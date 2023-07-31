use std::io::{BufReader, Cursor};

use xmlsql::{ParseOptions, Selector};

fn main() {
    let db = xmlsql::parse_to_temp_file(
        BufReader::new(Cursor::new(include_str!("./ex2.xml"))),
        ParseOptions {
            ignore_whitespace: true,
        },
    )
    .unwrap();

    let sel = Selector::new("ref[to]").unwrap();
    let refs = sel.match_all(&db).unwrap();

    println!("{:?}", &refs);
    let targets = refs
        .into_iter()
        .filter_map(|x| db.attr_by_name(x.node_id, "to", None).ok().flatten())
        .filter_map(|x| {
            Selector::new(&format!("[id='{}']", x.value))
                .unwrap()
                .match_one(&db)
                .unwrap()
        })
        .collect::<Vec<_>>();

    println!("{:?}", targets);
}
