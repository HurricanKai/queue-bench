use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use futures::future::join;
use queue_bench::{postgres_queue::PostgresQueue, redis_queue::RedisQueue, Queue};
use rsmq_async::{PoolOptions, RsmqOptions};

fn criterion_benchmark(c: &mut Criterion) {
    let runtime = &tokio::runtime::Runtime::new().unwrap();

    let postgres_url = "postgresql://postgres:changeme@localhost:5432/test-db";

    let pql = runtime
        .block_on(sqlx::PgPool::connect(postgres_url))
        .unwrap();

    let rsmq = runtime
        .block_on(rsmq_async::PooledRsmq::new(
            RsmqOptions {
                db: 0,
                host: "localhost".to_owned(),
                ns: "rsmq".to_owned(),
                password: None,
                port: "6379".to_owned(),
                realtime: false,
            },
            PoolOptions {
                ..Default::default()
            },
        ))
        .unwrap();

    const KB: usize = 1024;
    const CONFIGS: [i32; 1] = [500];
    for size in [1 * KB, 4 * KB, 1024 * KB] {
        for i in CONFIGS {
            let mut group = c.benchmark_group(format!("enqueue {size} bytes x {i}"));

            group.measurement_time(Duration::from_secs(60));
            group.sample_size(60);

            group.bench_with_input(
                BenchmarkId::new("redis", ""),
                &(i, size),
                |b, &(i, size)| {
                    b.to_async(runtime).iter(|| async {
                        let mut queue = RedisQueue::new(
                            rsmq.clone(),
                            uuid::Builder::from_random_bytes(rand::random())
                                .as_uuid()
                                .to_string(),
                        )
                        .await
                        .unwrap();
                        for _ in 0..i {
                            queue.push(ByteStruct::new(size)).await.unwrap();
                        }
                    });
                },
            );
            group.bench_with_input(
                BenchmarkId::new("postgres", ""),
                &(i, size),
                |b, &(i, size)| {
                    b.to_async(runtime).iter(|| async {
                        let mut queue = PostgresQueue::new(
                            pql.clone(),
                            uuid::Builder::from_random_bytes(rand::random())
                                .as_uuid()
                                .to_string(),
                        )
                        .await
                        .unwrap();
                        for _ in 0..i {
                            queue.push(ByteStruct::new(size)).await.unwrap();
                        }
                    });
                },
            );

            group.finish();

            let mut group = c.benchmark_group(format!("push then pull {size} bytes x {i}"));

            group.measurement_time(Duration::from_secs(60));
            group.sample_size(60);
            group.bench_with_input(
                BenchmarkId::new("redis", ""),
                &(i, size),
                |b, &(i, size)| {
                    b.to_async(runtime).iter(|| async {
                        let mut queue = RedisQueue::new(
                            rsmq.clone(),
                            uuid::Builder::from_random_bytes(rand::random())
                                .as_uuid()
                                .to_string(),
                        )
                        .await
                        .unwrap();
                        for _ in 0..i {
                            queue.push(ByteStruct::new(size)).await.unwrap();
                        }
                        for _ in 0..i {
                            (queue.try_pull().await.unwrap() as Option<ByteStruct>).unwrap();
                        }
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new("postgres", ""),
                &(i, size),
                |b, &(i, size)| {
                    b.to_async(runtime).iter(|| async {
                        let mut queue = PostgresQueue::new(
                            pql.clone(),
                            uuid::Builder::from_random_bytes(rand::random())
                                .as_uuid()
                                .to_string(),
                        )
                        .await
                        .unwrap();
                        for _ in 0..i {
                            queue.push(ByteStruct::new(size)).await.unwrap();
                        }
                        for _ in 0..i {
                            (queue.try_pull().await.unwrap() as Option<ByteStruct>).unwrap();
                        }
                    });
                },
            );

            group.finish();

            let mut group = c.benchmark_group(format!("push mix pull {size} bytes x {i}"));

            group.measurement_time(Duration::from_secs(60));
            group.sample_size(60);
            group.bench_with_input(
                BenchmarkId::new("redis", ""),
                &(i, size),
                |b, &(i, size)| {
                    b.to_async(runtime).iter(|| async {
                        let mut queue = RedisQueue::new(
                            rsmq.clone(),
                            uuid::Builder::from_random_bytes(rand::random())
                                .as_uuid()
                                .to_string(),
                        )
                        .await
                        .unwrap();
                        for _ in 0..i {
                            queue.push(ByteStruct::new(size)).await.unwrap();
                            (queue.try_pull().await.unwrap() as Option<ByteStruct>).unwrap();
                        }
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new("postgres", ""),
                &(i, size),
                |b, &(i, size)| {
                    b.to_async(runtime).iter(|| async {
                        let mut queue = PostgresQueue::new(
                            pql.clone(),
                            uuid::Builder::from_random_bytes(rand::random())
                                .as_uuid()
                                .to_string(),
                        )
                        .await
                        .unwrap();
                        for _ in 0..i {
                            queue.push(ByteStruct::new(size)).await.unwrap();
                            (queue.try_pull().await.unwrap() as Option<ByteStruct>).unwrap();
                        }
                    });
                },
            );

            group.finish();

            let mut group = c.benchmark_group(format!("push & pull concurrent {size} bytes x {i}"));

            group.measurement_time(Duration::from_secs(60));
            group.sample_size(60);
            group.bench_with_input(
                BenchmarkId::new("redis", ""),
                &(i, size),
                |b, &(i, size)| {
                    b.to_async(runtime).iter(|| async {
                        let queue = RedisQueue::new(
                            rsmq.clone(),
                            uuid::Builder::from_random_bytes(rand::random())
                                .as_uuid()
                                .to_string(),
                        )
                        .await
                        .unwrap();

                        let q = queue.clone();
                        let pusher = tokio::spawn(async move {
                            let mut queue = q;
                            for _ in 0..i {
                                queue.push(ByteStruct::new(size)).await.unwrap();
                            }
                        });
                        let q = queue.clone();
                        let puller = tokio::spawn(async move {
                            let mut queue = q;
                            for _ in 0..i {
                                while let None =
                                    queue.try_pull().await.unwrap() as Option<ByteStruct>
                                {
                                }
                            }
                        });

                        let (a, b) = join(pusher, puller).await;
                        a.unwrap();
                        b.unwrap();
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new("postgres", ""),
                &(i, size),
                |b, &(i, size)| {
                    b.to_async(runtime).iter(|| async {
                        let queue = PostgresQueue::new(
                            pql.clone(),
                            uuid::Builder::from_random_bytes(rand::random())
                                .as_uuid()
                                .to_string(),
                        )
                        .await
                        .unwrap();

                        let q = queue.clone();
                        let pusher = tokio::spawn(async move {
                            let mut queue = q;
                            for _ in 0..i {
                                queue.push(ByteStruct::new(size)).await.unwrap();
                            }
                        });
                        let q = queue.clone();
                        let puller = tokio::spawn(async move {
                            let mut queue = q;
                            for _ in 0..i {
                                while let None =
                                    queue.try_pull().await.unwrap() as Option<ByteStruct>
                                {
                                }
                            }
                        });

                        let (a, b) = join(pusher, puller).await;
                        a.unwrap();
                        b.unwrap();
                    });
                },
            );

            group.finish();
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
struct ByteStruct {
    vec: Vec<u8>,
}

impl ByteStruct {
    fn new(size: usize) -> Self {
        let mut v = Vec::with_capacity(size);
        v.fill(12);
        Self { vec: v }
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
