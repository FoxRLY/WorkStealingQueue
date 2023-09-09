use crate::Executable;
use std::collections::VecDeque;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, Scope};

fn div_ceil(a: usize, b: usize) -> usize {
    if b == 0 {
        panic!("DivisionByZero: b == 0")
    };
    if a.checked_add(b).is_none() {
        panic!("AdditionOverflow: a + b > usize::MAX")
    }
    (a + b - 1) / b
}

// Вспомогательная структура, хранящая задачи
struct TaskDeque {
    data: VecDeque<Box<dyn Executable>>,
}

impl TaskDeque {
    fn new() -> Self {
        TaskDeque {
            data: VecDeque::new(),
        }
    }

    fn pop_batch(&mut self, batch_size: usize) -> Vec<Box<dyn Executable>> {
        let mut batch = vec![];
        batch.reserve(batch_size);
        for _ in 0..batch_size {
            if let Some(task) = self.data.pop_front() {
                batch.push(task);
            } else {
                break;
            }
        }
        batch
    }

    fn push_batch(&mut self, new_tasks: Vec<Box<dyn Executable>>) {
        for new_task in new_tasks.into_iter() {
            self.data.push_back(new_task);
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn try_steal(
        &mut self,
        min_retain_size: usize,
        batch_size: usize,
    ) -> Result<Vec<Box<dyn Executable>>, ()> {
        if self.len() <= min_retain_size {
            return Err(());
        }
        let task_number_to_steal = if (self.len() - min_retain_size) > batch_size {
            batch_size
        } else {
            self.len() - min_retain_size
        };
        if task_number_to_steal < 1 {
            return Err(());
        }
        Ok(self.pop_batch(task_number_to_steal))
    }

    fn pop(&mut self) -> Option<Box<dyn Executable>> {
        self.data.pop_front()
    }
}

struct TaskDequeLocking {
    queue: TaskDeque,
    is_locked: bool,
}

impl TaskDequeLocking {
    fn new() -> Self {
        TaskDequeLocking {
            queue: TaskDeque::new(),
            is_locked: false,
        }
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
pub struct WorkStealingQueueParallel {
    main_queue: Arc<Mutex<TaskDequeLocking>>,
    task_addition_channel: Option<Receiver<Option<Box<dyn Executable>>>>,
    max_worker_count: usize,
    threshold: usize,
    batch_size: usize,
    min_retain_size: usize,
}

impl WorkStealingQueueParallel {
    /// Новая параллельная очередь
    ///
    /// let (queue, task_channel, stop_signal_channel) = WorkStealingQueueParallel::new(...);
    pub fn new(
        threshold: usize,
        max_worker_count: usize,
        min_retain_size: usize,
        batch_size: usize,
    ) -> (Self, Sender<Option<Box<dyn Executable>>>) {
        let main_queue = Arc::new(Mutex::new(TaskDequeLocking::new()));
        let (sender, receiver) = channel();
        (
            WorkStealingQueueParallel {
                main_queue,
                task_addition_channel: Some(receiver),
                max_worker_count,
                threshold,
                batch_size,
                min_retain_size,
            },
            sender,
        )
    }

    fn processor_thread_func(
        task_queues: Vec<Arc<Mutex<TaskDequeLocking>>>,
        worker_count: Arc<Mutex<usize>>,
        inner_index: usize,
        task_count: Arc<Mutex<usize>>,
        min_retain_size: usize,
        batch_size: usize,
    ) {
        *worker_count.lock().unwrap() += 1;
        loop {
            // Если в очереди процессора есть задачи, то выполняем их
            let new_task = task_queues[inner_index].lock().unwrap().queue.pop();
            if let Some(task) = new_task {
                task.execute();
                *task_count.lock().unwrap() -= 1;
            }
            // Иначе пытаемся украсть задачи у других потоков
            else {
                let work_status = task_queues.iter().enumerate().any(|queue| {
                    if queue.0 == inner_index {
                        return false;
                    }
                    let mut victim_queue = queue.1.lock().unwrap();
                    let task_steal_result =
                        victim_queue.queue.try_steal(min_retain_size, batch_size);
                    if let Ok(stolen_tasks) = task_steal_result {
                        let mut inner_queue = task_queues[inner_index].lock().unwrap();
                        inner_queue.queue.push_batch(stolen_tasks);
                        true
                    } else {
                        false
                    }
                });
                if !work_status {
                    break;
                }
            }
        }
        task_queues[inner_index].lock().unwrap().is_locked = false;
        *worker_count.lock().unwrap() -= 1;
    }

    fn main_thread_func<'a>(mut self, s: &'a Scope<'a, '_>) {
        // Канал для задач
        let mut task_receiver = self.task_addition_channel.take();

        // Число задач, которые сейчас выполняются
        let task_count = Arc::new(Mutex::new(0_usize));

        // Число процессоров, которые сейчас работают
        let worker_count = Arc::new(Mutex::new(0_usize));

        // Создаем очереди для процессоров
        let mut task_queues = vec![self.main_queue.clone()];
        for _ in 1..self.max_worker_count {
            let new_task_queue = Arc::new(Mutex::new(TaskDequeLocking::new()));
            task_queues.push(new_task_queue);
        }

        // Цикл создания задач
        loop {
            // Если канал ивентов существует
            if let Some(r) = &mut task_receiver {
                // Получаем новые ивенты
                let mut new_events: Vec<_> = r.try_iter().collect();
                // Если не получили новых ивентов - отправляем поток на ожидание
                if new_events.is_empty() {
                    let new_event = r.recv();
                    // Если во время ожидания канал был оборван, значит ивентов больше не будет
                    let new_event = if let Ok(event) = new_event {
                        event
                    } else {
                        break;
                    };
                    new_events.push(new_event);
                }
                // Получаем задания из канала
                let mut new_tasks = vec![];
                for event in new_events {
                    //Если получили задачу - берем ее в массив
                    if let Some(new_task) = event {
                        new_tasks.push(new_task);
                    }
                    // Если получили стоп-сигнал - обрываем канал и больше не принимаем задач
                    else {
                        let _ = task_receiver.take();
                        break;
                    }
                }
                // Добавляем задачи в очередь
                *task_count.lock().unwrap() += new_tasks.len();
                self.main_queue.lock().unwrap().queue.push_batch(new_tasks);
            }
            // Если канала не существует, значит уже пришел стоп-сигнал и мы выполнили все задачи
            else {
                break;
            }

            // Логика создания процессоров
            // Создаем новые процессоры, пока можем
            while Self::processor_creation_func(
                s,
                task_queues.clone(),
                worker_count.clone(),
                self.max_worker_count,
                task_count.clone(),
                self.threshold,
                self.min_retain_size,
                self.batch_size,
            ).is_ok() {}
        }
    }

    fn processor_creation_func<'a>(
        s: &'a Scope<'a, '_>,
        task_queues: Vec<Arc<Mutex<TaskDequeLocking>>>,
        worker_count: Arc<Mutex<usize>>,
        max_worker_count: usize,
        task_count: Arc<Mutex<usize>>,
        threshold: usize,
        min_retain_size: usize,
        batch_size: usize,
    ) -> Result<(), ()> {
        // Проверяем, можем ли мы создать еще процессоров
        if *worker_count.lock().unwrap() < max_worker_count {
            // И нужны ли они в принципе
            if div_ceil(*task_count.lock().unwrap(), threshold) > *worker_count.lock().unwrap() {
                // Если да, то начинаем поиск свободной очереди
                let mut queue_index: Option<usize> = None;

                // Пытаемся забронировать свободную очередь для нового процессора
                for task_queue in task_queues.iter().enumerate() {
                    let (index, queue) = task_queue;
                    let mut queue = queue.lock().unwrap();
                    if !queue.is_locked {
                        queue.is_locked = true;
                        queue_index = Some(index);
                        break;
                    }
                }

                if let Some(inner_index) = queue_index {
                    s.spawn(move || {
                        Self::processor_thread_func(
                            task_queues,
                            worker_count,
                            inner_index,
                            task_count,
                            min_retain_size,
                            batch_size,
                        );
                    });
                    return Ok(());
                }
            }
        }
        Err(())
    }

    /// Старт выполнения очереди
    pub fn start(self) {
        thread::scope(|s| {
            Self::main_thread_func(self, s);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::WorkStealingQueueParallel;
    use crate::Task;
    use std::thread;

    #[test]
    fn parallel_test() {
        let (sender, receiver) = std::sync::mpsc::channel();
        let (queue, task_channel) = WorkStealingQueueParallel::new(100, 5, 10, 100);
        let mut global = 0;
        // Имитация параллельной работы
        thread::scope(|s| {
            // Поток добавления задач
            s.spawn(move || {
                for _ in 0..300 {
                    let sender = sender.clone();
                    let new_task = Task::new(move || {
                        sender.send(1).unwrap();
                    });
                    task_channel.send(Some(Box::new(new_task))).unwrap();
                }
            });

            // Поток выполнения задач
            s.spawn(move || {
                queue.start();
            });

            while let Ok(val) = receiver.recv() {
                global += val;
            }
        });
        assert_eq!(global, 300);
    }
}
