use core::time;
use std::sync::{Arc, Mutex};
use workstealing::Task;
use workstealing::parallel::{WorkStealingQueueSynced, WorkStealingQueueParallel};
use std::thread;

fn main(){
    // В данном случае очередь имеет следующие свойства:
    // - На каждые 100 задач создается по потоку
    // - Максимальное кол-во потоков - 5
    // - Минимальное кол-во задач, при котором поток не отдаст свои задачи - 10
    // - Кража будет составлять до 100 задач за раз
    // println!("Синхронизированная очередь");
    // let mut queue = WorkStealingQueueSynced::new(10, 5, 10, 100);
    // for i in 0..352{
    //     let new_task = Task::new(i, |i|{
    //         println!("{i}");
    //         thread::sleep(time::Duration::from_millis(i%5*10));
    //     });
    //     queue.add_task(Box::new(new_task));
    // }
    // queue.execute_queue();
    //
    // println!("Параллельная очередь");
    // let (queue, task_channel, stop_signal_channel) = WorkStealingQueueParallel::new(10, 5, 10, 100);
    // thread::scope(|s|{
    //
    //     // Потоки добавления задач
    //     let task_channel_clone = task_channel.clone();
    //     s.spawn(move||{
    //         for i in 0..100 {
    //             let new_task = Task::new(i, |i|{
    //                 println!("{i}");
    //                 thread::sleep(time::Duration::from_millis(i%5*100));
    //             });
    //             task_channel_clone.send(Box::new(new_task)).unwrap();
    //         }
    //     });
    //     let task_channel_clone = task_channel.clone();
    //     s.spawn(move||{
    //         for i in 100..253 {
    //             let new_task = Task::new(i, |i|{
    //                 println!("{i}");
    //                 thread::sleep(time::Duration::from_millis(i%5*10));
    //             });
    //             task_channel_clone.send(Box::new(new_task)).unwrap();
    //         }
    //     });
    //     let task_channel_clone = task_channel.clone();
    //     s.spawn(move||{
    //         for i in 253..324 {
    //             let new_task = Task::new(i, |i|{
    //                 println!("{i}");
    //                 thread::sleep(time::Duration::from_millis(i%9*100));
    //             });
    //             task_channel_clone.send(Box::new(new_task)).unwrap();
    //         }
    //     });
    //
    //     // Исполняющий поток
    //     s.spawn(move||{
    //         queue.start();
    //     });
    //     stop_signal_channel.send(()).unwrap();
    // });

        let global = Arc::new(Mutex::new(0));
        let (queue, task_channel) = WorkStealingQueueParallel::new(100, 5, 10, 100);
        
        // Имитация параллельной работы
        thread::scope(|s|{
            
            // Поток добавления задач
            let global = global.clone();
            s.spawn(move||{
                for _ in 0..300{
                    let global = global.clone();
                    let new_task = Task::new((), move |_|{
                        *global.lock().unwrap() += 1; 
                    });
                    task_channel.send(Some(Box::new(new_task))).unwrap();
                }
            });
            
            // Поток выполнения задач
            s.spawn(move||{
                queue.start();
            });
        });
        println!("{}", *global.lock().unwrap());
}
