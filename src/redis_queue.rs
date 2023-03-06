use rsmq_async::RsmqConnection;

use crate::Queue;

#[derive(Clone)]
pub struct RedisQueue {
    pool: rsmq_async::PooledRsmq,
    queue_name: String,
}

#[async_trait::async_trait]
impl<T: 'static + Send + Sync + serde::Serialize + for<'de> serde::Deserialize<'de>>
    Queue<T, anyhow::Error> for RedisQueue
{
    async fn try_pull(&mut self) -> Result<Option<T>, anyhow::Error> {
        Ok(self
            .pool
            .pop_message(&self.queue_name)
            .await
            .map_err(|e| Into::<anyhow::Error>::into(e))?
            .map(|e| {
                let message: String = e.message;
                serde_json::from_str(&message).map_err(|e| Into::<anyhow::Error>::into(e))
            })
            .transpose()?)
    }

    async fn push(&mut self, value: T) -> Result<(), anyhow::Error> {
        self.pool
            .send_message(
                &self.queue_name,
                serde_json::to_string(&value).map_err(|e| Into::<anyhow::Error>::into(e))?,
                None,
            )
            .await
            .map_err(|e| Into::<anyhow::Error>::into(e))?;

        Ok(())
    }
}

impl RedisQueue {
    pub async fn new(
        mut pool: rsmq_async::PooledRsmq,
        queue_name: String,
    ) -> Result<Self, anyhow::Error> {
        pool.create_queue(&queue_name, None, None, None)
            .await
            .map_err(|e| Into::<anyhow::Error>::into(e))?;

        Ok(Self { pool, queue_name })
    }
}
