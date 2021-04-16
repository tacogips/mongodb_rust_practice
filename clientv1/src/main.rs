use anyhow::Result;
use mongodb::{
    bson::{doc, from_document, to_document, Bson, Document},
    options::{ClientOptions, DropCollectionOptions, StreamAddress, UpdateModifications},
    Client, Collection, Database,
};

use serde::{
    de::{DeserializeOwned, Error},
    Deserialize, Deserializer, Serialize,
};

#[derive(Deserialize, Serialize, Debug)]
struct User {
    id: String,
    name: String,
    reviewed_book_ids: Vec<String>,
}

impl From<Document> for User {
    fn from(d: Document) -> Self {
        from_document(d).unwrap()
    }
}

#[derive(Deserialize, Serialize, Debug)]
struct Book {
    id: String,
    name: String,
    reviews: Vec<Review>,
}

impl From<Document> for Book {
    fn from(d: Document) -> Self {
        from_document(d).unwrap()
    }
}

#[derive(Deserialize, Serialize, Debug)]
struct Review {
    user_id: String,
    text: String,
}

//just for convinience.
fn s(s: &str) -> String {
    s.to_string()
}

//just for convinience.
fn to_doc<T>(v: T) -> Document
where
    T: Serialize,
{
    match to_document(&v) {
        Ok(d) => d,
        Err(e) => panic!("failed to convert to a docuemnt {:?}", e),
    }
}

async fn create_users(client: &Client) -> Result<()> {
    let db = client.database("test_db");
    let user_coll = db.collection("users");
    user_coll
        .insert_many(
            vec![
                to_doc(User {
                    id: s("user_1"),
                    name: s("john"),
                    reviewed_book_ids: vec![],
                }),
                to_doc(User {
                    id: s("user_2"),
                    name: s("anna"),
                    reviewed_book_ids: vec![],
                }),
            ],
            None,
        )
        .await?;

    user_coll
        .insert_one(
            to_doc(User {
                id: s("user_3"),
                name: s("joseph"),
                reviewed_book_ids: vec![],
            }),
            None,
        )
        .await?;

    Ok(())
}

async fn create_books(client: &Client) -> Result<()> {
    let db = client.database("test_db");
    let book_coll = db.collection("books");
    book_coll
        .insert_one(
            to_doc(to_doc(Book {
                id: s("book_1"),
                name: s("The Hitchhiker's Guide to Somewhere"),
                reviews: vec![],
            })),
            None,
        )
        .await?;

    Ok(())
}

async fn find_users(client: &Client) -> Result<()> {
    let db = client.database("test_db");
    let user_coll = db.collection("users");
    let found = user_coll.find_one(Some(doc! {"id":"user_1"}), None).await?;
    let found = User::from(found.unwrap());
    assert_eq!(s("user_1"), found.id);
    println!("\nfound user:{:?}", found);
    Ok(())
}

async fn add_reviews_in_session(client: &Client) -> Result<()> {
    let db = client.database("test_db");
    let user_coll = db.collection("users");
    let book_coll = db.collection("books");

    let user_id = s("user_2");
    let book_id = s("book_1");
    {
        // TODO(tacogips) try to find a doc using indices.
        book_coll
            .update_one(
                doc! {"id" : book_id.clone()},
                UpdateModifications::Document(doc! {
                    "$push":{
                        "reviews":{
                            "user_id": user_id.clone(),
                            "text": s("Good reading")
                        },
                    }
                }),
                None,
            )
            .await?;

        user_coll
            .update_one(
                doc! {"id" : user_id.clone()},
                UpdateModifications::Document(doc! {
                    "$push":{
                        "reviewed_book_ids":book_id.clone(),
                    }
                }),
                None,
            )
            .await?;

        // another session
        let found = book_coll
            .find_one(Some(doc! {"id":book_id}), None)
            .await?
            .unwrap();

        // TODO(tacogips) Is the push operations above supposed to be invisible from another session yet?
        println!("\nupdated book:{:?}", found);

        let found = user_coll
            .find_one(Some(doc! {"id":user_id}), None)
            .await?
            .unwrap();

        println!("\nupdated user:{:?}", found);
    }

    Ok(())
}

async fn drop_colls(client: &Client) -> Result<()> {
    let db = client.database("test_db");
    let user_coll = db.collection("users");
    user_coll.drop(None).await?;

    let book_coll = db.collection("books");
    book_coll.drop(None).await?;
    Ok(())
}

#[tokio::main]
async fn main() {
    let opts = ClientOptions::builder()
        .hosts(vec![
            StreamAddress {
                hostname: "mongo1".to_string(),
                port: Some(30001),
            },
            StreamAddress {
                hostname: "mongo2".to_string(),
                port: Some(30002),
            },
            StreamAddress {
                hostname: "mongo3".to_string(),
                port: Some(30003),
            },
        ])
        .repl_set_name(Some("my-replica-set".to_string()))
        .build();

    let client = Client::with_options(opts).unwrap();
    create_users(&client).await.unwrap();
    create_books(&client).await.unwrap();
    find_users(&client).await.unwrap();
    add_reviews_in_session(&client).await.unwrap();
    drop_colls(&client).await.unwrap();
}
