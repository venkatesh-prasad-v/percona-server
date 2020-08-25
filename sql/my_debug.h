#ifndef CP_LOG_H
#define CP_LOG_H

#include <iostream>
#include <sstream>
#include <string>

extern bool cp_debug;
class THD;

#define MY_D(msg)                                                          \
  do {                                                                     \
    if (true) {                                                            \
      THD *curr_thd = current_thd;                                         \
      My_logger(curr_thd ? curr_thd->thread_id() : 0) << msg;              \
    }                                                                      \
  } while (0)

class My_logger {
 public:
  My_logger(uint32 id) : tid(id) {}
  ~My_logger() {
    std::cout << "[DEBUG] " << tid << " " << oss_.str() << std::endl;
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
