use sqlx::{Pool, Postgres};

use crate::Queue;

#[derive(Clone)]
pub struct PostgresQueue {
    pool: Pool<Postgres>,
    table_name: String,
}

#[derive(sqlx::FromRow, serde::Serialize, serde::Deserialize)]
struct Element {
    id: i64,
    value: serde_json::Value,
}

#[async_trait::async_trait]
impl<T: 'static + Unpin + Send + serde::Serialize + for<'de> serde::Deserialize<'de>>
    Queue<T, anyhow::Error> for PostgresQueue
{
    async fn try_pull(&mut self) -> Result<Option<T>, anyhow::Error> {
        let table_name = &self.table_name;
        Ok(sqlx::query_as::<_, Element>(&format!(
            "DELETE FROM \"{table_name}\"
            WHERE id = (
                SELECT (id)
                FROM \"{table_name}\"
                ORDER BY id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING *"
        ))
        .fetch_optional(&self.pool)
        .await?
        .map(|e| serde_json::from_value(e.value).unwrap()))
    }

    async fn push(&mut self, value: T) -> Result<(), anyhow::Error> {
        let table_name = &self.table_name;
        sqlx::query(&format!("INSERT INTO \"{table_name}\"(value) VALUES ($1)"))
            .bind(serde_json::to_value(value).map_err(|e| Into::<anyhow::Error>::into(e))?)
            .execute(&self.pool)
            .await
            .map_err(|e| Into::<anyhow::Error>::into(e))?;
        Ok(())
    }
}

impl PostgresQueue {
    pub async fn connect(url: &str, table_name: String) -> Result<Self, anyhow::Error> {
        let pool = Pool::<Postgres>::connect(url)
            .await
            .map_err(|e| Into::<anyhow::Error>::into(e))?;

        Ok(Self::new(pool, table_name).await?)
    }

    pub async fn new(pool: Pool<Postgres>, table_name: String) -> Result<Self, anyhow::Error> {
        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS \"{table_name}\"(id BIGSERIAL PRIMARY KEY, value JSON NOT NULL)"
        ))
        .execute(&pool)
        .await
        .map_err(|e| Into::<anyhow::Error>::into(e))?;

        Ok(Self { pool, table_name })
    }
}
