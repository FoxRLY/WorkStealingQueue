use std::collections::VecDeque;
use std::mem::replace;
use std::sync::{Arc, Mutex};
use std::thread::{self, Scope};
use std::sync::mpsc::{Sender, Receiver, channel};
use crate::Executable;


// 1) Перед началом главная очередь содержит задачи для выполнения
// 2) Начало: процессоры набирают задачи из главной очереди в свои очереди
// 3) Если процессор освободился, он начинает искать свободные задачи у других процессоров
// 4) Если нашел свободную задачу, то забирает ее у процессора и выполняет ее сам
// 5) Если не нашел, то идет в главную очередь и набирает задач из нее
// 6) Когда все процессоры закончат работу и в главной очереди не останется задач, конец

fn div_ceil(a: usize, b: usize) -> usize {
    if b == 0 {
        panic!("DivisionByZero: b == 0")
    };
    (a + b - 1) / b
}

// Вспомогательная структура, хранящая задачи
struct TaskDeque {
    data: VecDeque<Box<dyn Executable>>,
    deque_index: usize,
}

impl TaskDeque{
    fn new(index: usize) -> Self {
        TaskDeque { data: VecDeque::new() , deque_index: index}
    }

    fn pop_batch(&mut self, batch_size: usize) -> Vec<Box<dyn Executable>> {
        
        let mut batch = vec![];
        batch.reserve(batch_size);
        for _ in 0..batch_size {
            if let Some(task) = self.data.pop_front(){
                batch.push(task);
            }
            else{
                break;
            }
        }
        batch
    }

    fn push(&mut self, new_task: Box<dyn Executable>) {
        self.data.push_back(new_task);
    }

    fn push_batch(&mut self, new_tasks: Vec<Box<dyn Executable>>) {
        for new_task in new_tasks.into_iter(){
            self.data.push_back(new_task); 
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn try_steal(&mut self, min_retain_size: usize, batch_size: usize) -> Result<Vec<Box<dyn Executable>>, ()>{
        if self.len() <= min_retain_size{
            return Err(());
        }
        let task_number_to_steal = if (self.len() - min_retain_size) > batch_size{
            batch_size
        } else {
            self.len()-min_retain_size
        };
        if task_number_to_steal < 1 {
            return Err(());
        }
        Ok(self.pop_batch(task_number_to_steal))
    }

    fn pop(&mut self) -> Option<Box<dyn Executable>>{
        self.data.pop_front()
    }
}

/// Work-stealing очередь синхронизированная
///
/// * threshold - кол-во потоков = кол-во задач / threshold
/// * max_worker_count - максимальное кол-во потоков
/// * min_retain_size - минимальное количество задач, которые поток не будет отдавать другим.
/// * batch_size - максимальное кол-во задач, которое поток может украсть
///
/// Выбор переменных следует осуществлять в зависимости от выполняемых задач:
/// - Если threshold будет маленьким относительно количества задач, то
///   будет использовано слишком много потоков
/// - Если batch_size будет намного больше min_retain_size, то задачи будут менее эффективно
/// распределяться по очередям
/// - Если min_retain_size будет слишком большим и в очереди потока задач меньше batch_size, то
/// задачи будут выполняться менее эффективно.
pub struct WorkStealingQueueSynced{
    main_queue: Arc<Mutex<TaskDeque>>,
    workers: Vec<Arc<Mutex<TaskDeque>>>,
    threshold: usize,
    max_worker_count: usize,
    min_retain_size: usize,
    batch_size: usize,
}

impl WorkStealingQueueSynced{
    pub fn new(threshold: usize, max_worker_count: usize, min_retain_size: usize, batch_size: usize) -> Self {
        let main_queue = Arc::new(Mutex::new(TaskDeque::new(0)));
        let workers = vec![main_queue.clone()];
        WorkStealingQueueSynced { 
            main_queue,
            workers,
            threshold,
            max_worker_count,
            min_retain_size,
            batch_size
        }
    }

    /// Добавить задачу
    pub fn add_task(&mut self, new_task: Box<dyn Executable>){
        self.main_queue.lock().unwrap().push(new_task);
    }

    /// Функция определения необходимого числа процессоров
    fn count_workers(&self) -> usize {
        let new_value = self.main_queue.lock().unwrap().len() / self.threshold;
        if new_value > self.max_worker_count {
            self.max_worker_count
        }
        else{
            new_value
        }
    }

    /// Начать выполнение задач
    pub fn execute_queue(&mut self){
        // Создание необходимого числа очередей
        let worker_count = self.count_workers();
        for i in 1..worker_count {
            let new_worker = Arc::new(Mutex::new(TaskDeque::new(i)));
            self.workers.push(new_worker);
        }

        thread::scope(move |s|{
            // Каждой очереди отводится свой процессор
            for task_queue in &self.workers {
                // Каждая очередь имеет свой список других очередей 
                let mut workers = self.workers.clone();
                let deque_index = task_queue.lock().unwrap().deque_index;
                workers.swap_remove(deque_index);

                let inner_queue = task_queue.clone();
                let min_steal_size = self.min_retain_size;
                let batch_size = self.batch_size;
                s.spawn(move || {
                    loop{
                        // Если в очереди процессора есть задачи, выполняем их
                        let new_task = inner_queue.lock().unwrap().pop();
                        if let Some(task) = new_task{
                            task.execute();
                        }
                        // Иначе проходимся по очередям в поисках задач
                        else{
                            let work_status = workers.iter().any(|worker|{
                                let mut worker_guard = worker.lock().unwrap();
                                let task_steal_result = worker_guard.try_steal(min_steal_size, batch_size);
                                let victim_index = worker_guard.deque_index;
                                if let Ok(tasks) = task_steal_result{
                                    let mut inner_guard = inner_queue.lock().unwrap();
                                    println!("Queue {deque_index}: stolen {} tasks from queue {victim_index}", tasks.len());
                                    inner_guard.push_batch(tasks);
                                    true
                                }
                                else{
                                    false
                                }
                            });
                            // Если не нашли очередей, у которых можем украсть задачи, то выключаем
                            // процессор
                            if !work_status{
                                break;
                            }
                        }
                    }
                });
            }
        });
    }
}

struct TaskDequeLocking{
    queue: TaskDeque,
    is_locked: bool,
}

impl TaskDequeLocking{
    fn new(index: usize) -> Self {
        TaskDequeLocking { queue: TaskDeque::new(index), is_locked: false }
    }
}



/// Work-stealing очередь параллельная
///
/// * threshold - кол-во потоков = кол-во задач / threshold
/// * max_worker_count - максимальное кол-во потоков
/// * min_retain_size - минимальное количество задач, которые поток не будет отдавать другим.
/// * batch_size - максимальное кол-во задач, которое поток может украсть
///
/// Выбор переменных следует осуществлять в зависимости от выполняемых задач:
/// - Если threshold будет маленьким относительно количества задач, то
///   будет использовано слишком много потоков
/// - Если batch_size будет намного больше min_retain_size, то задачи будут менее эффективно
/// распределяться по очередям
/// - Если min_retain_size будет слишком большим и в очереди потока задач меньше batch_size, то
/// задачи будут выполняться менее эффективно.
pub struct WorkStealingQueueParallel{
    main_queue: Arc<Mutex<TaskDequeLocking>>,
    task_addition_channel: Option<Receiver<Box<dyn Executable>>>,
    end_signal_channel: Option<Receiver<()>>,
    max_worker_count: usize,
    threshold: usize,
    batch_size: usize,
    min_retain_size: usize,
}

impl WorkStealingQueueParallel{
    /// Новая параллельная очередь
    ///
    /// let (queue, task_channel, stop_signal_channel) = WorkStealingQueueParallel::new(...);
    pub fn new(threshold: usize, max_worker_count: usize, min_retain_size: usize, batch_size: usize) -> (Self, Sender<Box<dyn Executable>>, Sender<()>) {
        let main_queue = Arc::new(Mutex::new(TaskDequeLocking::new(0)));
        let (sender, receiver) = channel();
        let (end_sender, end_receiver) = channel();
        (
            WorkStealingQueueParallel {
                main_queue,
                task_addition_channel: Some(receiver),
                max_worker_count,
                end_signal_channel: Some(end_receiver),
                threshold, 
                batch_size,
                min_retain_size,
            },
            sender,
            end_sender,
        )
    }
    
    fn stop_signal_thread_func(end_signal_receiver:Receiver<()>, flag: Arc<Mutex<bool>>){
        // Ждем любого сигнала с канала и выставляем флаг окончания работы
        let _ = end_signal_receiver.recv();
        println!("Завершаю выполнение очереди");
        *flag.lock().unwrap() = true;
    } 
    
    fn task_receiving_thread_func(
        task_receiver: Receiver<Box<dyn Executable>>,
        main_queue: Arc<Mutex<TaskDequeLocking>>,
        task_count: Arc<Mutex<usize>>,
        flag_1: Arc<Mutex<bool>>,
        flag_2: Arc<Mutex<bool>>,
    ){
        loop{
             // Получаем задачи из канала
            let new_tasks: Vec<_> = task_receiver.try_iter().collect();
            if *flag_1.lock().unwrap() && new_tasks.is_empty(){
                println!("Очередь больше не получит новых заданий");
                *flag_2.lock().unwrap() = true;
                break;
            }
            // Иначе лочим главную очередь и счетчик задач
            let mut main_queue = main_queue.lock().unwrap();
            let mut task_count = task_count.lock().unwrap();

            // И добавляем задачи в очередь, обновляя счетчик
            *task_count += new_tasks.len();
            main_queue.queue.push_batch(new_tasks);
        }
    }

    fn processor_thread_func(
        task_queues: Vec<Arc<Mutex<TaskDequeLocking>>>,
        worker_count: Arc<Mutex<usize>>,
        inner_index: usize,
        task_count: Arc<Mutex<usize>>,
        min_retain_size: usize,
        batch_size: usize,
    ){
        *worker_count.lock().unwrap() += 1;
        loop{
            // Если в очереди процессора есть задачи, то выполняем их
            let new_task = task_queues[inner_index].lock().unwrap().queue.pop();
            if let Some(task) = new_task {
                task.execute();
                *task_count.lock().unwrap() -= 1;
            }
            // Иначе пытаемся украсть задачи у других потоков
            else{
                let work_status = task_queues.iter().enumerate().any(|queue|{
                if queue.0 == inner_index{
                    return false;
                }
                let mut victim_queue = queue.1.lock().unwrap();
                let task_steal_result = victim_queue.queue.try_steal(min_retain_size, batch_size);
                if let Ok(stolen_tasks) = task_steal_result{
                    let mut inner_queue = task_queues[inner_index].lock().unwrap();
                    inner_queue.queue.push_batch(stolen_tasks);
                    true
                }
                else{
                    false
                }
                });
                if !work_status{
                    break;
                }
            }
        }
        task_queues[inner_index].lock().unwrap().is_locked = false;
        *worker_count.lock().unwrap() -= 1;
    }

    fn scheduler_thread_func<'a>(
        s: &'a Scope<'a, '_>,
        task_queues: Vec<Arc<Mutex<TaskDequeLocking>>>,
        worker_count: Arc<Mutex<usize>>,
        task_count: Arc<Mutex<usize>>,
        min_retain_size: usize,
        batch_size: usize,
        max_worker_count: usize,
        threshold: usize,
        flag_2: Arc<Mutex<bool>>
    ){
        // Цикл создания процессоров
        loop{
            // Если очередь больше не получит новых задач и кол-во задач равно нулю -
            // выходим
            if *flag_2.lock().unwrap() && *task_count.lock().unwrap() == 0 {
                println!("Очередь отработала все задачи");
                break;
            }
                    
            // Иначе проверяем, можем ли мы создать еще одного работника
            if *worker_count.lock().unwrap() < max_worker_count{
                // И нужен ли он в принципе
                if div_ceil(*task_count.lock().unwrap(), threshold) > *worker_count.lock().unwrap() {
                    // Если да, то начинаем поиск свободной очереди
                    let task_queues = task_queues.clone();
                    let mut queue_index: Option<usize> = None;

                    // Пытаемся забронировать ее для нового процессора
                    for task_queue in task_queues.iter().enumerate(){
                        let (index, queue) = task_queue;
                        let mut queue = queue.lock().unwrap();
                        if !queue.is_locked{
                            queue.is_locked = true;
                            queue_index = Some(index);
                            break;
                        }
                    }
                    // Если смогли забронировать, то создаем новый процессор
                    if let Some(inner_index) = queue_index {
                        let task_count = task_count.clone();
                        let worker_count = worker_count.clone();
                        // Новый процессор
                        s.spawn(move ||{
                            Self::processor_thread_func(task_queues, worker_count, inner_index, task_count, min_retain_size, batch_size)
                        });
                    }
                } 
            }
        } 
    }

    /// Старт выполнения очереди
    pub fn start(mut self){
        // Канал для задач
        let task_receiver = self.task_addition_channel.take().unwrap();
        // let task_receiver = replace(&mut self.task_addition_channel, None).unwrap();

        // Канал для сигнала об окончании работы
        let end_signal_receiver = self.end_signal_channel.take().unwrap();

        // Флаг остановки работы
        let is_end_signal_received = Arc::new(Mutex::new(false));
        let is_end_signal_received_clone = is_end_signal_received.clone();

        // Флаг окончания добавления новых задач
        let is_queue_loading_stopped = Arc::new(Mutex::new(false));
        let is_queue_loading_stopped_clone = is_queue_loading_stopped.clone();

        thread::scope(|s|{
            // Поток для отслеживания стоп-сигнала
            s.spawn(move||{
                Self::stop_signal_thread_func(end_signal_receiver, is_end_signal_received_clone);
            });
            
            
            let main_queue = self.main_queue.clone();
            let task_count = Arc::new(Mutex::new(0_usize));
            let task_count_clone = task_count.clone();
            // Поток получения задач
            s.spawn(move||{
                Self::task_receiving_thread_func(
                    task_receiver, main_queue,
                    task_count_clone, is_end_signal_received,
                    is_queue_loading_stopped_clone
                );
            });

            // Создаем очереди для процессоров
            let mut task_queues = vec![self.main_queue.clone()];
            for i in 1..self.max_worker_count{
                let new_task_queue = Arc::new(Mutex::new(TaskDequeLocking::new(i)  ));
                task_queues.push(new_task_queue);
            }
            

            let worker_count = Arc::new(Mutex::new(0));
            s.spawn(move||{
                Self::scheduler_thread_func(
                    s, task_queues, worker_count,
                    task_count, self.min_retain_size,
                    self.batch_size, self.max_worker_count,
                    self.threshold, is_queue_loading_stopped
                );
            });
        });
    }
}

#[cfg(test)]
mod tests{
    use std::sync::{Arc, Mutex};
    use std::thread;
    use crate::Task;
    use super::{WorkStealingQueueSynced, WorkStealingQueueParallel};

    #[test]
    fn sync_test(){
        let global_counter = Arc::new(Mutex::new(0_u32));
        let mut queue = WorkStealingQueueSynced::new(100, 5, 10, 100);
        for _ in 0..300{
            let gc_clone = global_counter.clone();
            let new_task = Task::new((), move |_|{
                *gc_clone.lock().unwrap() += 1;
            });
            queue.add_task(Box::new(new_task));
        }
        queue.execute_queue();
        assert_eq!(*global_counter.lock().unwrap(), 300);
    }

    #[test]
    fn parallel_test(){
        let (sender, receiver) = std::sync::mpsc::channel();
        let (queue, task_channel, stop_signal_channel) = WorkStealingQueueParallel::new(100, 5, 10, 100);
        
        // Имитация параллельной работы
        thread::scope(|s|{
            
            // Поток добавления задач
            s.spawn(move||{
                for _ in 0..300{
                    let sender = sender.clone();
                    let new_task = Task::new((), move |_|{
                        sender.send(1).unwrap(); 
                    });
                    task_channel.send(Box::new(new_task)).unwrap();
                }
            });
            
            // Поток выполнения задач
            s.spawn(move||{
                queue.start();
            });
            
            let mut global = 0;
            while let Ok(val) = receiver.recv(){
                global += val;
            }
            assert_eq!(global, 300);
            
            // Сигнал остановки выполнения задач
            stop_signal_channel.send(()).unwrap();
        });
    }
}
