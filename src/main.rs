use core::time;
use workstealing::Task;
use workstealing::parallel::{WorkStealingQueueSynced, WorkStealingQueueParallel};
use std::thread;

fn main(){
    // В данном случае очередь имеет следующие свойства:
    // - На каждые 100 задач создается по потоку
    // - Максимальное кол-во потоков - 5
    // - Минимальное кол-во задач, при котором поток не отдаст свои задачи - 10
    // - Кража будет составлять до 100 задач за раз
    println!("Синхронизированная очередь");
    let mut queue = WorkStealingQueueSynced::new(100, 5, 10, 100);
    for i in 0..300{
        let new_task = Task::new(i, |i|{
            println!("{i}");
            thread::sleep(time::Duration::from_millis(i%5*100));
        });
        queue.add_task(Box::new(new_task));
    }
    queue.execute_queue();

    println!("Параллельная очередь");
    let (queue, task_channel, stop_signal_channel) = WorkStealingQueueParallel::new(100, 5, 10, 100);
    thread::scope(|s|{

        // Потоки добавления задач
        let task_channel_clone = task_channel.clone();
        s.spawn(move||{
            for i in 0..100 {
                let new_task = Task::new(i, |i|{
                    println!("{i}");
                    thread::sleep(time::Duration::from_millis(i%5*100));
                });
                task_channel_clone.send(Box::new(new_task)).unwrap();
            }
        });
        let task_channel_clone = task_channel.clone();
        s.spawn(move||{
            for i in 100..200 {
                let new_task = Task::new(i, |i|{
                    println!("{i}");
                    thread::sleep(time::Duration::from_millis(i%5*10));
                });
                task_channel_clone.send(Box::new(new_task)).unwrap();
            }
        });
        let task_channel_clone = task_channel.clone();
        s.spawn(move||{
            for i in 200..300 {
                let new_task = Task::new(i, |i|{
                    println!("{i}");
                    thread::sleep(time::Duration::from_millis(i%5*1000));
                });
                task_channel_clone.send(Box::new(new_task)).unwrap();
            }
        });

        // Исполняющий поток
        s.spawn(move||{
            queue.start();
        });
        stop_signal_channel.send(()).unwrap();
    });
}
