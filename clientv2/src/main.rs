use anyhow::{anyhow, Result};
use mongodb::{
    bson::{doc, Bson, Document},
    error::{Result as TxResult, TRANSIENT_TRANSACTION_ERROR, UNKNOWN_TRANSACTION_COMMIT_RESULT},
    options::{
        Acknowledgment, ClientOptions, DropCollectionOptions, ReadConcern, ServerAddress,
        TransactionOptions, UpdateModifications, WriteConcern,
    },
    Client, ClientSession, Collection, Database,
};

use serde::{
    de::{DeserializeOwned, Error},
    Deserialize, Deserializer, Serialize,
};

use futures::stream::TryStreamExt;

#[derive(Deserialize, Serialize, Debug, PartialEq)]
struct User {
    id: String,
    name: String,
    reviewed_book_ids: Vec<String>,
}

#[derive(Deserialize, Serialize, Debug, PartialEq)]
struct Book {
    id: String,
    name: String,
    reviews: Vec<Review>,
    authors: Vec<String>,
    supervisors: Vec<String>,
}

#[derive(Deserialize, Serialize, Debug, PartialEq)]
struct Review {
    user_id: String,
    text: String,
}

//just for convinience.
fn s(s: &str) -> String {
    s.to_string()
}

async fn create_users(client: &Client) -> Result<()> {
    let db = client.database("test_db");
    let user_coll = db.collection::<User>("users");
    if let Err(e) = user_coll.drop(None).await {
        println!("drop user coll error {:?}", e);
    }
    user_coll
        .insert_many(
            vec![
                User {
                    id: s("user_1"),
                    name: s("john"),
                    reviewed_book_ids: vec![],
                },
                User {
                    id: s("user_2"),
                    name: s("anna"),
                    reviewed_book_ids: vec![],
                },
            ],
            None,
        )
        .await?;

    user_coll
        .insert_one(
            User {
                id: s("user_3"),
                name: s("joseph"),
                reviewed_book_ids: vec![],
            },
            None,
        )
        .await?;

    Ok(())
}

async fn create_books(client: &Client) -> Result<()> {
    let db = client.database("test_db");
    let book_coll = db.collection::<Book>("books");

    if let Err(e) = book_coll.drop(None).await {
        println!("drop book coll error {:?}", e);
    }

    book_coll
        .insert_one(
            Book {
                id: s("book_1"),
                name: s("The Hitchhiker's Guide to Somewhere"),
                reviews: vec![],
                authors: vec![],
                supervisors: vec![],
            },
            None,
        )
        .await?;

    Ok(())
}

async fn find_users(client: &Client) -> Result<()> {
    let db = client.database("test_db");
    let user_coll = db.collection::<User>("users");
    let found = user_coll.find_one(Some(doc! {"id":"user_1"}), None).await?;
    let found = found.unwrap();
    assert_eq!(found.id, s("user_1"));
    println!("\nfound user:{:?}", found);
    Ok(())
}

async fn add_reviews_in_session(client: &Client) -> Result<()> {
    let db = client.database("test_db");
    let user_coll = db.collection::<User>("users");
    let book_coll = db.collection::<Book>("books");

    let user_id = s("user_2");
    let book_id = s("book_1");
    let mut session = client.start_session(None).await?;

    let tx_options = TransactionOptions::builder()
        .read_concern(ReadConcern::majority())
        .write_concern(WriteConcern::builder().w(Acknowledgment::Majority).build())
        .build();
    session.start_transaction(tx_options).await?;

    loop {
        {
            // TODO(tacogips) try to find a doc using indices.
            book_coll
                .update_one_with_session(
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
                    &mut session,
                )
                .await?;

            user_coll
                .update_one_with_session(
                    doc! {"id" : user_id.clone()},
                    UpdateModifications::Document(doc! {
                        "$push":{
                            "reviewed_book_ids":book_id.clone(),
                        }
                    }),
                    None,
                    &mut session,
                )
                .await?;
        }

        {
            // read from other session before commit
            let mut another_session = client.start_session(None).await?;
            let found = book_coll
                .find_one_with_session(
                    Some(doc! {"id":book_id.clone()}),
                    None,
                    &mut another_session,
                )
                .await?
                .unwrap();

            assert_eq!(found.reviews.len(), 0);
            println!("\nupdated book in another session:{:?}", found);

            let found = user_coll
                .find_one_with_session(
                    Some(doc! {"id":user_id.clone()}),
                    None,
                    &mut another_session,
                )
                .await?
                .unwrap();

            assert_eq!(found.reviewed_book_ids.len(), 0);
            println!("\nupdated user:{:?}", found);
        }

        match commit_tx(&mut session).await {
            Ok(_) => break,
            Err(e) => {
                if e.contains_label(TRANSIENT_TRANSACTION_ERROR) {
                    // TRANSIENT_TRANSACTION_ERROR  implies entire transaction can be retried
                    // see https://www.mongodb.com/blog/post/how-to-select--for-update-inside-mongodb-transactions for more detail
                    continue;
                } else {
                    return Err(anyhow!("{}", e));
                }
            }
        }
    }

    {
        // read from other session after commit
        let mut another_session = client.start_session(None).await?;
        let found = book_coll
            .find_one_with_session(
                Some(doc! {"id":book_id.clone()}),
                None,
                &mut another_session,
            )
            .await?
            .unwrap();

        assert_eq!(found.reviews.len(), 1);
        println!("\nupdated book in another session:{:?}", found);

        let found = user_coll
            .find_one_with_session(
                Some(doc! {"id":user_id.clone()}),
                None,
                &mut another_session,
            )
            .await?
            .unwrap();

        assert_eq!(found.reviewed_book_ids.len(), 1);
        println!("\nupdated user:{:?}", found);
    }

    Ok(())
}

async fn commit_tx(session: &mut ClientSession) -> TxResult<()> {
    loop {
        let result = session.commit_transaction().await;
        if let Err(ref error) = result {
            // rertry untiry the write concern will sarifified
            if error.contains_label(UNKNOWN_TRANSACTION_COMMIT_RESULT) {
                continue;
            }
        }
        break;
    }

    Ok(())
}

async fn abort_tx(client: &Client) -> Result<()> {
    let db = client.database("test_db");
    let book_coll = db.collection::<Book>("books");

    let book_id = s("book_fake");
    let mut session = client.start_session(None).await?;

    let tx_options = TransactionOptions::builder()
        .read_concern(ReadConcern::majority())
        .write_concern(WriteConcern::builder().w(Acknowledgment::Majority).build())
        .build();
    session.start_transaction(tx_options).await?;

    book_coll
        .insert_one_with_session(
            Book {
                id: book_id.clone(),
                name: s("ABC book"),
                reviews: vec![],
                authors: vec![],
                supervisors: vec![],
            },
            None,
            &mut session,
        )
        .await?;

    {
        let mut another_session = client.start_session(None).await?;
        let found_before_tx = book_coll
            .find_one_with_session(
                Some(doc! {"id":book_id.clone()}),
                None,
                &mut another_session,
            )
            .await?;
        assert!(found_before_tx.is_none());
    }

    session.abort_transaction().await?;

    {
        let mut another_session = client.start_session(None).await?;
        let found_before_tx = book_coll
            .find_one_with_session(
                Some(doc! {"id":book_id.clone()}),
                None,
                &mut another_session,
            )
            .await?;
        assert!(found_before_tx.is_none());
    }

    Ok(())
}

async fn drop_colls(client: &Client) -> Result<()> {
    let db = client.database("test_db");
    let user_coll = db.collection::<User>("users");
    user_coll.drop(None).await?;

    let book_coll = db.collection::<Book>("books");
    book_coll.drop(None).await?;
    Ok(())
}

async fn misc(client: &Client) -> Result<()> {
    let db = client.database("test_db");
    let book_coll = db.collection::<Book>("books");

    if let Err(e) = book_coll.drop(None).await {
        println!("drop book coll error {:?}", e);
    }

    book_coll
        .insert_one(
            Book {
                id: s("book_with_authors"),
                name: s("some book"),
                reviews: vec![],
                authors: vec![s("author_1"), s("author_2")],
                supervisors: vec![],
            },
            None,
        )
        .await?;

    // search by id
    {
        let found = book_coll
            .find_one(doc! {"id":"book_with_authors"}, None)
            .await;
        assert!(found.is_ok());
        let found = found.unwrap();
        assert_eq!(
            found,
            Some(Book {
                id: s("book_with_authors"),
                name: s("some book"),
                reviews: vec![],
                authors: vec![s("author_1"), s("author_2")],
                supervisors: vec![],
            }),
        )
    }

    // search by $in
    {
        let found = book_coll
            .find_one(doc! {"authors" :{"$in":["author_1"]}}, None)
            .await;
        assert!(found.is_ok());
        let found = found.unwrap();
        assert_eq!(
            found,
            Some(Book {
                id: s("book_with_authors"),
                name: s("some book"),
                reviews: vec![],
                authors: vec![s("author_1"), s("author_2")],
                supervisors: vec![],
            }),
        )
    }

    // search by $in not found
    {
        let found = book_coll
            .find_one(doc! {"authors" :{"$in":["imaginary_author_1"]}}, None)
            .await;
        assert!(found.is_ok());
        let found = found.unwrap();
        assert_eq!(found, None)
    }

    // search by $in with empty vec
    {
        let found = book_coll.find_one(doc! {"authors" :{"$in":[]}}, None).await;
        assert!(found.is_ok());
        let found = found.unwrap();
        assert_eq!(found, None,)
    }

    // search by $in with empty vec
    {
        let found = book_coll.find(doc! {"authors" :{"$in":[]}}, None).await;
        assert!(found.is_ok());
        let found = found.unwrap();
        let found: Vec<Book> = found.try_collect().await?;
        assert!(found.is_empty())
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    let opts = ClientOptions::builder()
        .hosts(vec![
            ServerAddress::Tcp {
                host: "localhost".to_string(),
                port: Some(30001),
            },
            ServerAddress::Tcp {
                host: "localhost".to_string(),
                port: Some(30002),
            },
            ServerAddress::Tcp {
                host: "localhost".to_string(),
                port: Some(30003),
            },
        ])
        .repl_set_name("my-replica-set".to_string())
        .build();

    let client = Client::with_options(opts).unwrap();
    create_users(&client).await.unwrap();
    create_books(&client).await.unwrap();
    find_users(&client).await.unwrap();
    add_reviews_in_session(&client).await.unwrap();

    abort_tx(&client).await.unwrap();
    misc(&client).await.unwrap();

    drop_colls(&client).await.unwrap();
}
