use std::sync::{Arc, Mutex};
use std::thread;
use workstealing::parallel::WorkStealingQueueParallel;
use workstealing::Task;

fn main() {
    // В данном случае очередь имеет следующие свойства:
    // - На каждые 100 задач создается по потоку
    // - Максимальное кол-во потоков - 5
    // - Минимальное кол-во задач, при котором поток не отдаст свои задачи - 10
    let global = Arc::new(Mutex::new(0));
    let (queue, task_channel) = WorkStealingQueueParallel::new(100, 5, 10, 100);

    // Имитация параллельной работы
    thread::scope(|s| {
        let global = global.clone();
        // Поток добавления задач
        s.spawn(move || {
            for _ in 0..300 {
                let global = global.clone();
                let new_task = Task::new(move || {
                    *global.lock().unwrap() += 1;
                });
                task_channel.send(Some(Box::new(new_task))).unwrap();
            }
        });

        // Поток выполнения задач
        s.spawn(move || {
            queue.start();
        });
    });
    println!("{}", *global.lock().unwrap());
}
