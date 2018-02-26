

#ifndef _TASKING_SYNC_COUNTER_H_
#define _TASKING_SYNC_COUNTER_H_

#include "../base/base.h"
#include <atomic>

SMILE_NS_BEGIN

class SyncCounter final {
public:
  SyncCounter();
  ~SyncCounter() = default;

  /**
   * @brief Sets the value of the counter
   *
   * @param value
   */
  void setValue(int32_t value);

  /**
   * @brief Fetches and Increments the value of the counter
   *
   * @return 
   */
  int32_t fetch_increment();

  /**
   * @brief Fetches and Decrements the value of the counter
   */
  int32_t fetch_decrement();

  /**
   * @brief Blocks until the counter is set to zero
   */
  void join();

private:

  std::atomic<int32_t> m_counter;

};

SMILE_NS_END

#endif /* ifndef _SMILE_TASKING_SYNC_COUNTER_H_ */
