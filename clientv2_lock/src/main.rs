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
use tokio;

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

//just for convinience.
fn s(s: &str) -> String {
    s.to_string()
}

const DB_NAME: &str = "tx_test_db";

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
        let result = update_users_name(&db, &mut session, "user_1", "update_in_session1").await;

        println!("session 1 result {:?}", result);

        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        commit_tx(&mut session).await.unwrap();

        let db = cloned_client.database(DB_NAME);
        let user_coll = db.collection::<User>("users");
        let found = user_coll
            .find_one_with_session(Some(doc! {"id":"user_1".clone()}), None, &mut session)
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
        let result = update_users_name(&db, &mut session, "user_1", "update_in_session2").await;

        assert!(result.is_err());
        println!("write conflict error :{:?}", result.err());
    }

    jh.await??;

    Ok(())
}

async fn update_users_name(
    db: &Database,
    session: &mut ClientSession,
    user_id: &str,
    name: &str,
) -> Result<()> {
    let user_coll = db.collection::<User>("users");

    {
        user_coll
            .update_one_with_session(
                doc! {"id" : user_id.clone()},
                UpdateModifications::Document(doc! {
                    "$set":{

                        "name": name,
                    }
                }),
                None,
                session,
            )
            .await?;
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
    let db = client.database(DB_NAME);
    let user_coll = db.collection::<User>("users");
    user_coll.drop(None).await?;

    Ok(())
}

async fn create_users(client: &Client) -> Result<()> {
    let db = client.database(DB_NAME);
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

    conflict_updating(&client).await.unwrap();

    drop_colls(&client).await.unwrap();
}
