#include <algorithm>
#include <mutex>

#include "hazard_ptr.h"

#ifndef HAZARD_FLUSH_SIZE
#define HAZARD_FLUSH_SIZE 16
#endif

namespace hazard {
namespace detail {

namespace {

class HazardRoot {
 public:
  HazardRoot() : hazard_record_head_(nullptr), hazard_bucket_head_(nullptr) {
  }

  HazardRoot(const HazardRoot&) = delete;
  HazardRoot& operator=(const HazardRoot&) = delete;

  ~HazardRoot();

  HazardRecord* allocateRecord();

  void deallocateRecord(HazardRecord* record);

  HazardBucket* allocateBucket();

  void flushRetired(RetiredItems& retired);

 private:
  typedef std::vector<const void*> ScanedSet;

  void scanHp(ScanedSet& scaned);

  static void deleteItems(const ScanedSet& scaned, RetiredItems& retired);

  HazardRecord* hazard_record_head_;
  HazardBucket* hazard_bucket_head_;
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

  HazardBucket* bucket = hazard_bucket_head_;
  while (bucket) {
    HazardBucket* next = bucket->next;
    delete bucket;
    bucket = next;
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
      assert(record->hp_reserved == 0);
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
  // hp_buckets内の全てのHazardBucketを空き状態(active == 0)としてマークする。
  atomic::atomic_fence_release();
  for (HazardBucket* bucket : record->hp_buckets) {
    atomic::atomic_store_relaxed(&bucket->active, 0);
  }
  record->hp_buckets.clear();

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

HazardBucket*
HazardRoot::allocateBucket() {
  // bucket->active == 0 なものがあるか調べ、あれば active を 1 にして返す。
  HazardBucket* bucket = atomic::atomic_load_acquire(&hazard_bucket_head_);
  while (bucket) {
    int active = atomic::atomic_load_relaxed(&bucket->active);
    if (active == 0 && atomic::atomic_compare_and_set(&bucket->active, 0, 1)) {
      return bucket;
    }
    bucket = bucket->next;
  }

  // 既存の HazardBucket は全て使用中だったので、
  // 新しい HazardBucket を生成し hazard_bucket_head_ に繋げてから返す。
  bucket = new HazardBucket();
  HazardBucket* head;
  do {
    head = atomic::atomic_load_relaxed(&hazard_bucket_head_);
    bucket->next = head;
  } while (!atomic::atomic_compare_and_set(&hazard_bucket_head_, head, bucket));
  return bucket;
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
  HazardBucket* bucket = atomic::atomic_load_acquire(&hazard_bucket_head_);
  while (bucket) {
    for (const hp_t& h : bucket->hp) {
      const void* p = atomic::atomic_load_relaxed(&h);
      if (p)
        scaned.push_back(p);
    }
    bucket = bucket->next;
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

__thread HazardRecord* local_record(nullptr);

}

HazardRecord::HazardRecord() : hp_reserved(0), active(1) {
  retired.reserve(HAZARD_FLUSH_SIZE);
}

HazardRecord&
HazardRecord::getLocalRecord() {
  HazardRecord* record = local_record;
  if (!record) {
    record = hazard_root.allocateRecord();
    local_record = record;
  }
  return *record;
}

void
HazardRecord::clearLocalRecord() {
  HazardRecord* record = local_record;
  if (record) {
    assert(record->hp_reserved == 0);
    local_record = nullptr;
    hazard_root.deallocateRecord(record);
  }
}

std::size_t
HazardRecord::reserveHp(std::size_t num) {
  std::size_t start = hp_reserved;
  std::size_t end = start + num;
  if (end > hp_buckets.size() * HAZARD_BUCKET_SIZE) {
    std::size_t needs = (end - (hp_buckets.size() * HAZARD_BUCKET_SIZE) +
                         HAZARD_BUCKET_SIZE - 1) / HAZARD_BUCKET_SIZE;
    hp_buckets.reserve(hp_buckets.size() + needs);
    for (std::size_t i = 0; i < needs; i++)
      hp_buckets.push_back(hazard_root.allocateBucket());
  }
  hp_reserved = end;
  return start;
}

void
HazardRecord::returnHp(std::size_t start, std::size_t num) {
  atomic::atomic_fence_release();
  for (std::size_t i = 0; i < num; i++)
    atomic::atomic_store_relaxed(getHp(start + i),
                                 static_cast<const void*>(nullptr));
  hp_reserved -= num;
  assert(start == hp_reserved);
}

void
HazardRecord::addRetired(void* obj, void* alloc, deleter_func del) {
  if (!obj) return;
  retired.push_back({obj, alloc, del});
  if (retired.size() >= HAZARD_FLUSH_SIZE)
    hazard_root.flushRetired(retired);
}

}
}
