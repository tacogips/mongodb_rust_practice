use anyhow::{anyhow, Result};
use mongodb::{
    bson::{doc, Bson, Document},
    error::{Result as TxResult, TRANSIENT_TRANSACTION_ERROR, UNKNOWN_TRANSACTION_COMMIT_RESULT},
    options::{
        Acknowledgment, ClientOptions, DropCollectionOptions, ReadConcern, ServerAddress,
        TransactionOptions, UpdateModifications, WriteConcern,
    },
    results::UpdateResult,
    Client, ClientSession, Collection, Database,
};
use tokio;

use serde::{
    de::{DeserializeOwned, Error},
    Deserialize, Deserializer, Serialize,
};

#[derive(Deserialize, Serialize, Debug)]
struct Book {
    id: String,
    name: String,
    version: i64,
}

//just for convinience.
fn s(s: &str) -> String {
    s.to_string()
}

const DB_NAME: &str = "tx_test_db_1";
const COLL_NAME: &str = "books";

async fn conflict_updating(client: &Client) -> Result<()> {
    //let mut session = client.start_session(None).await?;

    let cloned_client = client.clone();
    let jh: tokio::task::JoinHandle<Result<()>> = tokio::task::spawn(async move {
        let db = cloned_client.database(DB_NAME);
        let mut session = cloned_client.start_session(None).await?;
        let tx_options = TransactionOptions::builder()
            .read_concern(ReadConcern::majority())
            .write_concern(WriteConcern::builder().w(Acknowledgment::Majority).build())
            .build();
        session.start_transaction(tx_options).await?;

        println!("{:?}", "start session 1");
        let result = update_users_name(&db, &mut session, "book_1", "update_in_session1", 1).await;

        println!("session 1 result {:?}", result);

        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        commit_tx(&mut session).await.unwrap();

        let db = cloned_client.database(DB_NAME);
        let book_coll = db.collection::<Book>(COLL_NAME);
        let found = book_coll
            .find_one_with_session(Some(doc! {"id":"book_1".clone()}), None, &mut session)
            .await
            .unwrap()
            .unwrap();
        println!("found in session 1:{:?}", found);
        Ok(())
    });

    {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let db = client.database(DB_NAME);
        let mut session = client.start_session(None).await.unwrap();
        let tx_options = TransactionOptions::builder()
            .read_concern(ReadConcern::majority())
            .write_concern(WriteConcern::builder().w(Acknowledgment::Majority).build())
            .build();
        session.start_transaction(tx_options).await.unwrap();

        println!("{:?}", "start session 2");

        let book_coll = db.collection::<Book>(COLL_NAME);
        let found = book_coll
            .find_one_with_session(Some(doc! {"id":"book_1".clone()}), None, &mut session)
            .await
            .unwrap()
            .unwrap();
        println!("found in session 2 before update:{:?}", found);

        let result = update_users_name(&db, &mut session, "book_1", "update_in_session2", 1).await;

        assert!(result.is_err());
        println!("write conflict error :{:?}", result.err());
    }

    {
        tokio::time::sleep(std::time::Duration::from_secs(4)).await;
        let db = client.database(DB_NAME);
        let mut session = client.start_session(None).await.unwrap();
        let tx_options = TransactionOptions::builder()
            .read_concern(ReadConcern::majority())
            .write_concern(WriteConcern::builder().w(Acknowledgment::Majority).build())
            .build();
        session.start_transaction(tx_options).await.unwrap();

        println!("{:?}", "start session 3");

        let book_coll = db.collection::<Book>(COLL_NAME);
        let found = book_coll
            .find_one_with_session(Some(doc! {"id":"book_1".clone()}), None, &mut session)
            .await
            .unwrap()
            .unwrap();
        println!("found in session 3 before update:{:?}", found);

        let result = update_users_name(&db, &mut session, "book_1", "update_in_session3", 1)
            .await
            .unwrap();

        assert_eq!(result.matched_count, 0);
        assert_eq!(result.modified_count, 0);
    }

    jh.await??;

    Ok(())
}

async fn update_users_name(
    db: &Database,
    session: &mut ClientSession,
    book_id: &str,
    name: &str,
    version: i64,
) -> Result<UpdateResult> {
    let book_coll = db.collection::<Book>(COLL_NAME);

    let result = book_coll
        .update_one_with_session(
            doc! {"$and":[
                {"id" : book_id.clone()},
                {"version":version}
            ]},
            UpdateModifications::Document(doc! {
                "$set":{
                    "name": name,
                },
                "$inc":{
                    "version": 1,
                }
            }),
            None,
            session,
        )
        .await?;

    Ok(result)
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
    let db = client.database(DB_NAME);
    let book_coll = db.collection::<Book>(COLL_NAME);
    book_coll.drop(None).await?;

    Ok(())
}

async fn create_books(client: &Client) -> Result<()> {
    let db = client.database(DB_NAME);
    let book_coll = db.collection::<Book>(COLL_NAME);
    if let Err(e) = book_coll.drop(None).await {
        println!("drop user coll error {:?}", e);
    }
    book_coll
        .insert_many(
            vec![
                Book {
                    id: s("book_1"),
                    name: s("john"),
                    version: 1,
                },
                Book {
                    id: s("book_2"),
                    name: s("anna"),
                    version: 1,
                },
            ],
            None,
        )
        .await?;

    book_coll
        .insert_one(
            Book {
                id: s("book_3"),
                name: s("joseph"),
                version: 1,
            },
            None,
        )
        .await?;

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
    create_books(&client).await.unwrap();

    conflict_updating(&client).await.unwrap();

    drop_colls(&client).await.unwrap();
}
