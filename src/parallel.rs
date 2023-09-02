use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::thread;
use crate::Executable;


// 1) Перед началом главная очередь содержит задачи для выполнения
// 2) Начало: процессоры набирают задачи из главной очереди в свои очереди
// 3) Если процессор освободился, он начинает искать свободные задачи у других процессоров
// 4) Если нашел свободную задачу, то забирает ее у процессора и выполняет ее сам
// 5) Если не нашел, то идет в главную очередь и набирает задач из нее
// 6) Когда все процессоры закончат работу и в главной очереди не останется задач, конец


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

/// Work-stealing очередь
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
    main_queue: Arc<Mutex<TaskDeque>>,
    workers: Vec<Arc<Mutex<TaskDeque>>>,
    threshold: usize,
    max_worker_count: usize,
    min_retain_size: usize,
    batch_size: usize,
}

impl WorkStealingQueueParallel{
    pub fn new(threshold: usize, max_worker_count: usize, min_retain_size: usize, batch_size: usize) -> Self {
        let main_queue = Arc::new(Mutex::new(TaskDeque::new(0)));
        let workers = vec![main_queue.clone()];
        WorkStealingQueueParallel { 
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
                                // На время трансферта задач следует заблокировать свою очередь,
                                // иначе другие процессоры могут преждевременно выйти, не увидев задачи,
                                // которые были украдены, но не добавлены в очередь вора
                                let mut inner_guard = inner_queue.lock().unwrap();
                                let mut worker_guard = worker.lock().unwrap();
                                let task_steal_result = worker_guard.try_steal(min_steal_size, batch_size);
                                let victim_index = worker_guard.deque_index;
                                drop(worker_guard);
                                if let Ok(tasks) = task_steal_result{
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
