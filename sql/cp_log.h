#ifndef CP_LOG_H
#define CP_LOG_H

#include <iosfwd>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>

extern bool cp_debug;
class THD;

static std::mutex cp_mutex;

#define CP_DEBUG(msg)                                                      \
  do {                                                                     \
    if (cp_debug) {                                                        \
      THD *curr_thd = current_thd;                                         \
      Clone_persister_logger(curr_thd ? curr_thd->thread_id() : 0) << msg; \
    }                                                                      \
  } while (0)

class Clone_persister_logger {
 public:
  Clone_persister_logger(uint32 id) : tid(id) {}
  ~Clone_persister_logger() {
    cp_mutex.lock();
    std::cout << "[DEBUG] " << tid << " " << oss_.str() << std::endl;
    cp_mutex.unlock();
  }
  template <typename T>
  std::ostream &operator<<(const T &val) {
    return (oss_ << val);
  }

 private:
  std::ostringstream oss_;
  uint32 tid;
};

#endif /* CP_LOG_H */
