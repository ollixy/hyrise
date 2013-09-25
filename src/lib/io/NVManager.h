#ifdef PERSISTENCY_NVRAM

#ifndef SRC_LIB_IO_NVMANAGER_H
#define SRC_LIB_IO_NVMANAGER_H

#include <atomic>
#include <cstdint>
#include <stdexcept>
#include <mutex>

namespace hyrise {
namespace io {

typedef struct {
  uint64_t uuid;
  size_t size;
  size_t capacity;
  size_t allocated;
} NVVectorInfo;

class NVManager {
public:
  NVManager(const NVManager&) = delete;
  NVManager &operator=(const NVManager&) = delete;

  static void setNonVolatileMode() {
    if(_instanceCreated) {
      throw std::runtime_error("Can't set non-volatile mode after NVManager has been initiated");
    }
    _nonVolatileMode = true;
  }

  static NVManager &getInstance() {
    static NVManager instance;
    _instanceCreated = true;
    return instance;
  }
  void setVolatileMode(bool enabled=false);
  bool isInVolatileMode() const;
  void reset();
  NVVectorInfo* getOrCreateVectorSpace(uint64_t uuid,
                                       size_t size);
  NVVectorInfo* resizeVectorSpace(NVVectorInfo* vi,
                                  size_t newSize);
  void destroyVectorSpace(NVVectorInfo *vi);
  void persistMemoryArea(void* start,
                         size_t len);

#ifndef NDEBUG
  void verifyCanaries();
#else
  void verifyCanaries() {};
#endif

private:
  NVManager();
  void _initialize();
  uint64_t _generateUUID();
  NVVectorInfo* _allocateVectorSpace(size_t size);
  NVVectorInfo* _retrieveVectorInfo(uint64_t uuid) const;
  bool _pmfsMounted();

  std::atomic<uint64_t> _uuidCounter;
  static bool _instanceCreated, _nonVolatileMode;

  // pmem handle
  void *_pmp;

  std::mutex _allocateLock;
};

}
}

#endif // SRC_LIB_IO_NVMANAGER_H
#endif

