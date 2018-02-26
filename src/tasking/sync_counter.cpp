

#include "sync_counter.h"
#include "tasking.h"
#include <thread>
#include <chrono>

SMILE_NS_BEGIN

SyncCounter::SyncCounter() {
  m_counter.store(0);
}

void SyncCounter::setValue(int32_t value) {
  m_counter.store(value);
}

int32_t SyncCounter::fetch_increment() {
  return m_counter.fetch_add(1);
}

int32_t SyncCounter::fetch_decrement() {
  return m_counter.fetch_add(-1);
}

void SyncCounter::join() {
  while(m_counter.load() != 0) {
    if(getCurrentThreadId() == INVALID_THREAD_ID) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    } else {
      yield();
    }
  }
}

SMILE_NS_END

