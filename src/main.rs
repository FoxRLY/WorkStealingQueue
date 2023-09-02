use core::time;
use workstealing::Task;
use workstealing::parallel::WorkStealingQueueParallel;
use std::thread;

fn main(){
    // В данном случае очередь имеет следующие свойства:
    // - На каждые 100 задач создается по потоку
    // - Максимальное кол-во потоков - 5
    // - Минимальное кол-во задач, при котором поток не отдаст свои задачи - 10
    // - Кража будет составлять до 100 задач за раз
    let mut queue = WorkStealingQueueParallel::new(100, 5, 10, 100);
    for i in 0..300{
        let new_task = Task::new(i, |i|{
            println!("{i}");
            thread::sleep(time::Duration::from_millis(i%5*100));
        });
        queue.add_task(Box::new(new_task));
    }
    queue.execute_queue();
}
