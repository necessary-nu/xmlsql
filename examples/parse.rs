use std::io::{BufReader, Cursor};

use xmlsql::select::Selector;

fn main() {
    let db = xmlsql::parse_in_memory(
        BufReader::new(Cursor::new(include_str!("../example.xml"))),
    ).unwrap();

    let sel = Selector::new("a").unwrap();
    let matches = sel.match_all(&db);
    println!("All <a> elements: {:?}", &matches);

    for m in matches {
        println!("Node: {:?}", m);
        println!("- Attrs: {:?}", db.attrs(m.node_id).unwrap());
        println!("- Child: {:?}", db.child_nodes(m.node_id).unwrap());
    }

    println!(
        "Document top-level children: {:?}",
        db.document_child_nodes().unwrap()
    );

    println!("Root: {:?}", db.root().unwrap());
    println!("- Attrs: {:?}", db.attrs(1).unwrap());
    println!("- Child: {:?}", db.child_nodes(1).unwrap());

    let sel = Selector::new("f").unwrap();
    let matches = sel.match_all(&db);
    println!("All <f> elements: {:?}", &matches);
    println!("- Attrs: {:?}", db.attrs(matches[0].node_id).unwrap());

    let sel = Selector::new("f[attr1='potato']").unwrap();
    let matches = sel.match_all(&db);
    println!("All f[attr1='potato'] elements: {:?}", &matches);

    let sel = Selector::new("f[potato|attr2]").unwrap();
    let matches = sel.match_all(&db);
    println!("All f[potato|attr2] elements: {:?}", &matches);

    let sel = Selector::new("root > a").unwrap();
    let matches = sel.match_all(&db);
    println!("All root > a elements: {:?}", &matches);

    let sel = Selector::new("e > a").unwrap();
    let matches = sel.match_all(&db);
    println!("All e > a elements: {:?}", &matches);
}
