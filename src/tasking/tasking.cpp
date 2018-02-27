
#include "sync_counter.h"
#include "task_pool.h"
#include "tasking.h"
#include <atomic>
#include <chrono>
#include <queue>
#include <thread>

#include <iostream>


SMILE_NS_BEGIN

/**
 * @brief Atomic booleans to control threads running
 */
static std::atomic<bool>*           m_isRunning = nullptr;;

/**
 * @brief The number of threads in the thread pool
 */
static std::size_t                  m_numThreads = 0;

/**
 * @brief Vector of running threads objects
 */
static std::thread**                p_threads;

/**
 * @brief Pointer to the task pool with the tasks pending to start 
 */
static TaskPool*                    p_toStartTaskPool = nullptr;

/**
 * @brief Vectors holding the running tasks of the thread.
 */
static TaskPool*                    p_runningTaskPool = nullptr;


/**
 * @brief Thread local variable to store the id of the current thread
 */
static thread_local uint32_t        m_currentThreadId = INVALID_THREAD_ID;

/**
 * @brief Array of Contexts used to store the main context of each thread to
 * fall back to it when yielding a task
 */
static ExecutionContext*            m_threadMainContexts = nullptr;


/**
 * @brief Pointer to the thread local currently running task 
 */
static thread_local TaskContext*    p_currentRunningTask = nullptr;

/**
 * @brief Finalizes the current running task and releases its resources
 */
static void finalizeCurrentRunningTask() {
  if(p_currentRunningTask->m_finished) {
    if(p_currentRunningTask->p_syncCounter != nullptr) {
      int32_t last = p_currentRunningTask->p_syncCounter->fetch_decrement();
      /*if(last == 1) {
        if(p_currentRunningTask->p_parent != nullptr) {
          p_runningTaskPool->addTask(m_currentThreadId, p_currentRunningTask->p_parent);
        }
      }*/
    }
    delete p_currentRunningTask; // TODO: If a pool is used, reset the task and put it into the pool
  } else {
    p_runningTaskPool->addTask(m_currentThreadId, p_currentRunningTask);
  }
  p_currentRunningTask = nullptr;
}

/**
 * @brief Start the execution to the given task. A lambda function is passed
 * to the callcc method, which takes as input the current execution context.
 * This current execution context is stored int he m_threadMainContexts array
 * for the current thread. Then the task is executed. 
 *
 * @param task The task to execute
 */
static void startTask(TaskContext* taskContext) noexcept {
  p_currentRunningTask = taskContext; // TODO: A pool should be used here
  p_currentRunningTask->m_context = ctx::callcc([taskContext](ExecutionContext&& context) {
              m_threadMainContexts[m_currentThreadId] = std::move(context);
              taskContext->m_task.p_fp(taskContext->m_task.p_args);
              p_currentRunningTask->m_finished  = true;
              return std::move(m_threadMainContexts[m_currentThreadId]);
              }
             );
  finalizeCurrentRunningTask();
}


/**
 * @brief Resumes the given running task 
 *
 * @param runningTask The task to resume
 */
static void resumeTask(TaskContext* runningTask) {
  p_currentRunningTask = runningTask;
  p_currentRunningTask->m_context = p_currentRunningTask->m_context.resume();
  finalizeCurrentRunningTask();
}

/**
 * @brief The main function that each thread runs
 *
 * @param threadId The id of the thread that runs the function
 */
static void threadFunction(int threadId) noexcept {

  m_currentThreadId = threadId;

  while(m_isRunning[m_currentThreadId].load()) {
    TaskContext* task = p_toStartTaskPool->getNextTask(m_currentThreadId);
    if(task != nullptr) {
      startTask(task);
    } else if ( (task = p_runningTaskPool->getNextTask(m_currentThreadId)) != nullptr) {
      resumeTask(task);
    } else {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }
}

void startThreadPool(std::size_t numThreads) noexcept {

  p_toStartTaskPool     = new TaskPool{numThreads};
  p_runningTaskPool     = new TaskPool{numThreads};
  m_threadMainContexts  = new ExecutionContext[numThreads];
  m_numThreads          = numThreads;
  m_isRunning           = new std::atomic<bool>[m_numThreads];
  p_threads             = new std::thread*[m_numThreads];

  for(std::size_t i = 0; i < m_numThreads; ++i) {
    m_isRunning[i].store(true);
    p_threads[i] = new std::thread(threadFunction, i);
  }
}

void stopThreadPool() noexcept {

  // Telling threads to stop
  for(std::size_t i = 0; i < m_numThreads; ++i) {
    m_isRunning[i].store(false);
  }

  // Waitning threads to stop
  for(std::size_t i = 0; i < m_numThreads; ++i) {
    p_threads[i]->join();
  }

  delete [] p_threads;
  delete [] m_isRunning;
  delete [] m_threadMainContexts;
  delete p_runningTaskPool;
  delete p_toStartTaskPool;
}

void executeTaskAsync(uint32_t threadId, Task task, SyncCounter* counter ) noexcept {
  TaskContext* taskContext = new TaskContext{task, counter};
  if(taskContext->p_syncCounter != nullptr) {
    taskContext->p_syncCounter->fetch_increment();
  }

  if(getCurrentThreadId() != INVALID_THREAD_ID ) {
    taskContext->p_parent = p_currentRunningTask;
  } else {
    taskContext->p_parent = nullptr;
  }

  p_toStartTaskPool->addTask(threadId, taskContext);
} 

void executeTaskSync(uint32_t threadId, 
                     Task task, 
                     SyncCounter* counter) noexcept {
  executeTaskAsync(threadId, task, counter);
  counter->join();
}

uint32_t getCurrentThreadId() noexcept {
  return m_currentThreadId;
}

void yield() {
  assert(m_currentThreadId != INVALID_THREAD_ID);
  m_threadMainContexts[m_currentThreadId] = m_threadMainContexts[m_currentThreadId].resume();
}

SMILE_NS_END
