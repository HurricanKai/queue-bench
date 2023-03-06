use std::{future::IntoFuture, time::Duration};

use criterion::{
    criterion_group, criterion_main, measurement::WallTime, BenchmarkGroup, BenchmarkId, Criterion,
};
use futures::future::join;
use queue_bench::{
    local_queue::LocalQueue, postgres_queue::PostgresQueue, redis_queue::RedisQueue, Queue,
};
use rsmq_async::{PoolOptions, RsmqOptions};

fn criterion_benchmark(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    fn run_configs_on_group<
        T: 'static + Queue<ByteStruct, E> + Clone + Send + Sync,
        E: std::fmt::Debug,
        F1: Fn() -> Fut,
        Fut: IntoFuture<Output = T>,
    >(
        runtime: &tokio::runtime::Runtime,
        mut group: BenchmarkGroup<WallTime>,
        connect: F1,
    ) {
        group.measurement_time(Duration::from_secs(60));
        group.sample_size(60);

        const KB: usize = 1024;
        const CONFIGS: [i32; 1] = [100];
        for size in [1 * KB, 4 * KB, 1024 * KB] {
            for i in CONFIGS {
                group.bench_with_input(
                    BenchmarkId::new("enqueue", format!("{size} bytes x {i}")),
                    &(i, size),
                    |b, &(i, size)| {
                        b.to_async(runtime).iter(|| async {
                            let mut queue = connect().await;
                            for _ in 0..i {
                                queue.push(ByteStruct::new(size)).await.unwrap();
                            }
                        });
                    },
                );
            }
            for i in CONFIGS {
                group.bench_with_input(
                    BenchmarkId::new("push then pull", format!("{size} bytes x {i}")),
                    &(i, size),
                    |b, &(i, size)| {
                        b.to_async(runtime).iter(|| async {
                            let mut queue = connect().await;
                            for _ in 0..i {
                                queue.push(ByteStruct::new(size)).await.unwrap();
                            }
                            for _ in 0..i {
                                queue.try_pull().await.unwrap().unwrap();
                            }
                        });
                    },
                );
            }
            for i in CONFIGS {
                group.bench_with_input(
                    BenchmarkId::new("push mix pull", format!("{size} bytes x {i}")),
                    &(i, size),
                    |b, &(i, size)| {
                        b.to_async(runtime).iter(|| async {
                            let mut queue = connect().await;
                            for _ in 0..i {
                                queue.push(ByteStruct::new(size)).await.unwrap();
                                queue.try_pull().await.unwrap().unwrap();
                            }
                        });
                    },
                );
            }
            for i in CONFIGS {
                group.bench_with_input(
                    BenchmarkId::new("push & pull concurrent", format!("{size} bytes x {i}")),
                    &(i, size),
                    |b, &(i, size)| {
                        b.to_async(runtime).iter(|| async {
                            let mut queue = connect().await;

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
                                    while let None = queue.try_pull().await.unwrap() {}
                                }
                            });

                            let (a, b) = join(pusher, puller).await;
                            a.unwrap();
                            b.unwrap();
                        });
                    },
                );
            }
        }

        group.finish();
    }

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

    run_configs_on_group::<RedisQueue, _, _, _>(
        &runtime,
        c.benchmark_group("redis queue"),
        || async {
            RedisQueue::new(
                rsmq.clone(),
                uuid::Builder::from_random_bytes(rand::random())
                    .as_uuid()
                    .to_string(),
            )
            .await
            .unwrap()
        },
    );

    run_configs_on_group::<PostgresQueue, _, _, _>(
        &runtime,
        c.benchmark_group("postgres queue"),
        || async {
            PostgresQueue::new(
                pql.clone(),
                uuid::Builder::from_random_bytes(rand::random())
                    .as_uuid()
                    .to_string(),
            )
            .await
            .unwrap()
        },
    );

    run_configs_on_group::<LocalQueue<_>, _, _, _>(
        &runtime,
        c.benchmark_group("local queue"),
        || async { LocalQueue::new().unwrap() },
    );
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
