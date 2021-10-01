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

#[derive(Deserialize, Serialize, Debug)]
struct User {
    id: String,
    name: String,
    reviewed_book_ids: Vec<String>,
}

#[derive(Deserialize, Serialize, Debug)]
struct Book {
    id: String,
    name: String,
    reviews: Vec<Review>,
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
    assert_eq!(s("user_1"), found.id);
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

            assert_eq!(0, found.reviews.len());
            println!("\nupdated book in another session:{:?}", found);

            let found = user_coll
                .find_one_with_session(
                    Some(doc! {"id":user_id.clone()}),
                    None,
                    &mut another_session,
                )
                .await?
                .unwrap();

            assert_eq!(0, found.reviewed_book_ids.len());
            println!("\nupdated user:{:?}", found);
        }

        match commit_tx(&mut session).await {
            Ok(_) => break,
            Err(e) => {
                if e.contains_label(TRANSIENT_TRANSACTION_ERROR) {
                    // entire transaction can be retried
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

        assert_eq!(1, found.reviews.len());
        println!("\nupdated book in another session:{:?}", found);

        let found = user_coll
            .find_one_with_session(
                Some(doc! {"id":user_id.clone()}),
                None,
                &mut another_session,
            )
            .await?
            .unwrap();

        assert_eq!(1, found.reviewed_book_ids.len());
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

async fn drop_colls(client: &Client) -> Result<()> {
    let db = client.database("test_db");
    let user_coll = db.collection::<User>("users");
    user_coll.drop(None).await?;

    let book_coll = db.collection::<Book>("books");
    book_coll.drop(None).await?;
    Ok(())
}

#[tokio::main]
async fn main() {
    let opts = ClientOptions::builder()
        .hosts(vec![
            ServerAddress::Tcp {
                host: "mongo1".to_string(),
                port: Some(30001),
            },
            ServerAddress::Tcp {
                host: "mongo2".to_string(),
                port: Some(30002),
            },
            ServerAddress::Tcp {
                host: "mongo3".to_string(),
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
    drop_colls(&client).await.unwrap();
}
