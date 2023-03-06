use std::{collections::VecDeque, convert::Infallible, sync::Arc};

use crate::Queue;

#[derive(Clone)]
pub struct LocalQueue<T> {
    queue: Arc<futures::lock::Mutex<VecDeque<T>>>,
}

#[async_trait::async_trait]
impl<T: Send> Queue<T, Infallible> for LocalQueue<T> {
    async fn try_pull(&mut self) -> Result<Option<T>, Infallible> {
        let mut guard = self.queue.lock().await;

        Ok(guard.pop_front())
    }

    async fn push(&mut self, value: T) -> Result<(), Infallible> {
        let mut guard = self.queue.lock().await;
        guard.push_back(value);

        Ok(())
    }
}

impl<T> LocalQueue<T> {
    pub fn new() -> Result<Self, Infallible> {
        Ok(Self {
            queue: Arc::new(futures::lock::Mutex::new(VecDeque::new())),
        })
    }
}
