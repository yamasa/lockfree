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

  void flushRetired(HazardRecord* record);

 private:
  bool scanHp(ScanedSet& scaned);

  static void deleteItems(const ScanedSet& scaned, RetiredItems& retired);

  static void deleteAllItems(RetiredItems& retired);

  HazardRecord* hazard_record_head_;
  HazardBucket* hazard_bucket_head_;
};

HazardRoot::~HazardRoot() {
  HazardRecord* record = hazard_record_head_;
  while (record) {
    deleteAllItems(record->retired);
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
}

HazardRecord*
HazardRoot::allocateRecord() {
  // record->active == 0 なものがあるか調べ、あれば active を 1 にして返す。
  HazardRecord* record = atomic::atomic_load_acquire(&hazard_record_head_);
  while (record) {
    int active = atomic::atomic_load_relaxed(&record->active);
    if (active == 0 && atomic::atomic_compare_and_set(&record->active, 0, 1)) {
      assert(record->buckets_in_use == 0);
      assert(record->hp_buckets.empty());
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
  assert(record->buckets_in_use == 0);

  // hp_buckets内の全てのHazardBucketを空き状態(active == 0)としてマークする。
  atomic::atomic_fence_release();
  for (HazardBucket* bucket : record->hp_buckets) {
    atomic::atomic_store_relaxed(&bucket->active, 0);
  }
  record->hp_buckets.clear();

  // retired 内のオブジェクトをできる限りdeleteする。
  // (deleteしきれずに残ってしまってもよい。)
  if (!record->retired.empty()) {
    flushRetired(record);
  }

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
HazardRoot::flushRetired(HazardRecord* record) {
  try {
    if (scanHp(record->scaned)) {
      deleteItems(record->scaned, record->retired);
    } else {
      deleteAllItems(record->retired);
    }
  } catch(...) {
  }
}

bool
HazardRoot::scanHp(ScanedSet& scaned) {
  atomic::atomic_fence_seq_cst();
  scaned.clear();
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
    return false;

  std::sort(scaned.begin(), scaned.end());
  scaned.erase(std::unique(scaned.begin(), scaned.end()), scaned.end());
  return true;
}

void
HazardRoot::deleteItems(const ScanedSet& scaned, RetiredItems& retired) {
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

void
HazardRoot::deleteAllItems(RetiredItems& retired) {
  for (auto item : retired)
    item.doDelete();
  retired.clear();
}

HazardRoot hazard_root;

__thread HazardRecord* local_record(nullptr);

}

HazardRecord::HazardRecord() : buckets_in_use(0), active(1) {
  retired.reserve(HAZARD_FLUSH_SIZE);
}

HazardRecord&
HazardRecord::getLocalRecord(std::size_t num_buckets) {
  HazardRecord* record = local_record;
  if (!record) {
    record = hazard_root.allocateRecord();
    local_record = record;
  }
  HazardBuckets& buckets = record->hp_buckets;
  num_buckets += record->buckets_in_use;
  if (buckets.size() < num_buckets) {
    buckets.reserve(num_buckets);
    for (std::size_t i = buckets.size(); i < num_buckets; i++)
      buckets.push_back(hazard_root.allocateBucket());
  }
  return *record;
}

void
HazardRecord::clearLocalRecord() {
  HazardRecord* record = local_record;
  if (record) {
    local_record = nullptr;
    hazard_root.deallocateRecord(record);
  }
}

void
HazardRecord::addRetired(void* obj, void* alloc, deleter_func del) {
  if (!obj) return;
  retired.push_back({obj, alloc, del});
  if (retired.size() >= HAZARD_FLUSH_SIZE)
    hazard_root.flushRetired(this);
}

}
}
