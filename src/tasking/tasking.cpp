
#include "task_pool.h"
#include "tasking.h"
#include <chrono>
#include <atomic>
#include <thread>
#include <queue>

#include <boost/context/continuation.hpp>

namespace ctx = boost::context;
using Context = ctx::continuation;

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
 * @brief Pointer to the task pool where threads fetch tasks from
 */
static TaskPool*                    p_taskPool = nullptr;


/**
 * @brief Thread local variable to store the id of the current thread
 */
static thread_local uint32_t        m_currentThreadId = 0;

/**
 * @brief Array of Contexts used to store the main context of each thread to
 * fall back to it when yielding a task
 */
static Context*                     m_threadMainContexts = nullptr;


/**
 * @brief Structure used to store all the data regarding a running task
 */
struct RunningTask {
  Context      m_context;
  bool         m_finished;
  RunningTask* p_parent;
};

/**
 * @brief Vectors holding the running tasks of the thread.
 */
static std::queue<RunningTask*>*        m_runningTasks = nullptr;

static thread_local RunningTask*       m_currentRunningTask = nullptr;

/**
 * @brief Start the execution to the given task. A lambda function is passed
 * to the callcc method, which takes as input the current execution context.
 * This current execution context is stored int he m_threadMainContexts array
 * for the current thread. Then the task is executed. 
 *
 * @param task The task to execute
 */
static void startToTask(Task* task) noexcept {
  m_currentRunningTask = new RunningTask(); // TODO: A pool should be used here
  m_currentRunningTask->m_context = ctx::callcc([task](Context&& context) {
              m_threadMainContexts[m_currentThreadId] = std::move(context);
              m_currentRunningTask->p_parent    = nullptr;
              m_currentRunningTask->m_finished  = false;
              task->p_fp(task->p_args);
              m_currentRunningTask->m_finished = true;
              return std::move(m_threadMainContexts[m_currentThreadId]);
              }
             );
  if(m_currentRunningTask->m_finished) {
    delete m_currentRunningTask; // TODO: If a pool is used, reset the task and put it into the pool
    m_currentRunningTask = nullptr;
  } else {
    m_runningTasks[m_currentThreadId].push(m_currentRunningTask);
  }
}

/**
 * @brief Searches for a running task for the given thread
 *
 * @param threadId The thread to look for a running task
 *
 * @return The running task. nullptr if it does not exist. 
 */
static RunningTask* findRunningTask(int threadId) {
  if (m_runningTasks[threadId].size() == 0) {
    return nullptr;
  }

  RunningTask* runningTask = m_runningTasks[threadId].front();
  m_runningTasks[threadId].pop();
  return runningTask;
}

static void resumeTask(RunningTask* runningTask) {
  m_currentRunningTask = runningTask;
  m_currentRunningTask->m_context = runningTask->m_context.resume();

  if(m_currentRunningTask->m_finished) {
    delete m_currentRunningTask; // TODO: If a pool is used, reset the task and put it into the pool
    m_currentRunningTask = nullptr;
  } else {
    m_runningTasks[m_currentThreadId].push(m_currentRunningTask);
  }
}

/**
 * @brief The main function that each thread runs
 *
 * @param threadId The id of the thread that runs the function
 */
static void threadFunction(int threadId) noexcept {

  m_currentThreadId = threadId;

  while(m_isRunning[m_currentThreadId].load()) {
    Task task;
    RunningTask* runningTask = nullptr;
    if(p_taskPool->getNextTask(m_currentThreadId, &task)) {
      startToTask(&task);
    } else if ( (runningTask = findRunningTask(m_currentThreadId)) != nullptr) {
        resumeTask(runningTask);
    } else {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }
}

void startThreadPool(std::size_t numThreads) noexcept {

  p_taskPool            = new TaskPool{numThreads};
  m_threadMainContexts  = new Context[numThreads];
  m_runningTasks        = new std::queue<RunningTask*>[numThreads];
  m_numThreads          = numThreads;
  m_isRunning           = new std::atomic<bool>[m_numThreads];
  p_threads             = new std::thread*[m_numThreads];

  for(std::size_t i = 0; i < m_numThreads; ++i) {
    m_isRunning[i].store(true);
    p_threads[i] = new std::thread(threadFunction, i);
  }
}

void stopThreadPool() noexcept {

  for(std::size_t i = 0; i < m_numThreads; ++i) {
    m_isRunning[i].store(false);
  }

  for(std::size_t i = 0; i < m_numThreads; ++i) {
    p_threads[i]->join();
  }

  delete [] p_threads;
  delete [] m_isRunning;
  delete [] m_runningTasks;
  delete [] m_threadMainContexts;
  delete p_taskPool;
}

void executeTask(Task task, uint32_t threadId) noexcept {
  p_taskPool->addTask(threadId, task);
} 

uint32_t getCurrentThreadId() noexcept {
  return m_currentThreadId;
}

void yield() {
  m_threadMainContexts[m_currentThreadId] = m_threadMainContexts[m_currentThreadId].resume();
}

SMILE_NS_END
