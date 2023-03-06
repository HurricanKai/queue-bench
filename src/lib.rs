pub mod local_queue;
pub mod postgres_queue;
pub mod redis_queue;

#[async_trait::async_trait]
pub trait Queue<T, E> {
    async fn try_pull(&mut self) -> Result<Option<T>, E>;
    async fn push(&mut self, value: T) -> Result<(), E>;
}
