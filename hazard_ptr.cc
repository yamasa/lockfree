#include <algorithm>
#include <mutex>

#include "hazard_ptr.h"

namespace hazard {
namespace detail {

namespace {

class HazardRoot {
 public:
  HazardRoot() : hazard_record_head_(nullptr) {
  }

  HazardRoot(const HazardRoot&) = delete;
  HazardRoot& operator=(const HazardRoot&) = delete;

  ~HazardRoot();

  HazardRecord* allocateRecord();

  void deallocateRecord(HazardRecord* record);

  void flushRetired(RetiredItems& retired);

 private:
  typedef std::vector<const void*> ScanedSet;

  void scanHp(ScanedSet& scaned);

  static void deleteItems(const ScanedSet& scaned, RetiredItems& retired);

  HazardRecord* hazard_record_head_;
  std::mutex global_retired_mutex_;
  RetiredItems global_retired_;
};

HazardRoot::~HazardRoot() {
  HazardRecord* record = hazard_record_head_;
  while (record) {
    HazardRecord* next = record->next;
    delete record;
    record = next;
  }
  for (auto item : global_retired_)
    item.doDelete();
}

HazardRecord*
HazardRoot::allocateRecord() {
  // record->active == 0 なものがあるか調べ、あれば active を 1 にして返す。
  HazardRecord* record = atomic::atomic_load_acquire(&hazard_record_head_);
  while (record) {
    int active = atomic::atomic_load_relaxed(&record->active);
    if (active == 0 && atomic::atomic_compare_and_set(&record->active, 0, 1)) {
      assert(record->retired.empty());
      return record;
    }
    record = record->next;
  }

  // 既存の HazardRecord は全て使用中だったので、
  // 新しい HazardRecord を生成し hazard_record_head_ に繋げてから返す。
  record = new HazardRecord();
  HazardRecord* head;
  do {
    head = atomic::atomic_load_relaxed(&hazard_record_head_);
    record->next = head;
  } while (!atomic::atomic_compare_and_set(&hazard_record_head_, head, record));
  return record;
}

void
HazardRoot::deallocateRecord(HazardRecord* record) {
  // 最後にもう一度 scan を行い、それでも record->retired に残ったものを
  // global_retired_ に移しかえる。
  ScanedSet scaned;
  {
    std::lock_guard<std::mutex> lk(global_retired_mutex_);
    scanHp(scaned);
    deleteItems(scaned, record->retired);
    deleteItems(scaned, global_retired_);
    global_retired_.insert(global_retired_.end(),
                           record->retired.begin(), record->retired.end());
  }
  record->retired.clear();

  // record を空き状態(active == 0)としてマークする。
  atomic::atomic_store_release(&record->active, 0);
}

void
HazardRoot::flushRetired(RetiredItems& retired) {
  ScanedSet scaned;
  scanHp(scaned);
  deleteItems(scaned, retired);
}

void
HazardRoot::scanHp(ScanedSet& scaned) {
  atomic::atomic_fence_seq_cst();
  HazardRecord* record = atomic::atomic_load_acquire(&hazard_record_head_);
  while (record) {
    for (const hp_t& h : record->hp) {
      const void* p = atomic::atomic_load_relaxed(&h);
      if (p)
        scaned.push_back(p);
    }
    record = record->next;
  }
  atomic::atomic_fence_acquire();

  if (scaned.empty())
    return;
  std::sort(scaned.begin(), scaned.end());
  scaned.erase(std::unique(scaned.begin(), scaned.end()), scaned.end());
}

void
HazardRoot::deleteItems(const ScanedSet& scaned, RetiredItems& retired) {
  if (scaned.empty()) {
    for (auto item : retired)
      item.doDelete();
    retired.clear();
    return;
  }

  // この実装では、スキャンしたハザードポインタの内容を std::vector に格納し、
  // sort → unique → binary_search という手順でdelete可能かチェックしている。
  // しかし、代わりにOpen Addressing方式のHash Tableや、Bloom Filterを使うと
  // より効率的かもしれない。
  retired.erase(
      std::remove_if(
          retired.begin(), retired.end(),
          [&scaned] (RetiredItem& item) -> bool {
            if (std::binary_search(scaned.begin(), scaned.end(), item.object))
              return false;
            else {
              item.doDelete();
              return true;
            }
          }),
      retired.end());
}

HazardRoot hazard_root;

__thread LocalData* local_data(nullptr);

}

LocalData::LocalData(std::size_t threshold)
    : hazard_record_(hazard_root.allocateRecord()),
      flush_threshold_(threshold), hp_used_(0) {
  hazard_record_->retired.reserve(threshold);
  assert(local_data == nullptr);
  local_data = this;
}

LocalData::~LocalData() {
  assert(hp_used_ == 0);
  assert(local_data == this);
  local_data = nullptr;
  hazard_root.deallocateRecord(hazard_record_);
}

LocalData&
LocalData::getLocalData() {
  LocalData* data = local_data;
  assert(data != nullptr);
  return *data;
}

void
LocalData::addRetired(void* obj, void* alloc, deleter_func del) {
  if (!obj) return;
  RetiredItems& retired = hazard_record_->retired;
  retired.push_back({obj, alloc, del});
  if (retired.size() >= flush_threshold_)
    hazard_root.flushRetired(retired);
}

}
}
