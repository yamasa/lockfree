#ifndef HAZARD_PTR_H
#define HAZARD_PTR_H

#include <array>
#include <cassert>
#include <cstddef>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

// atomic関係については、tagged_ptr版に合わせるために、
// C++0xの std::atomic ではなく独自実装版を使用する。
#include "atomic.h"

// 一つのHazardBucketに格納されるハザードポインタの数。
#ifndef HAZARD_BUCKET_SIZE
#define HAZARD_BUCKET_SIZE 2
#endif

#ifndef CACHE_LINE_SIZE
#define CACHE_LINE_SIZE 64
#endif

namespace hazard {
namespace detail {

typedef void (*deleter_func)(void*);

struct RetiredItem {
  void* object;
  deleter_func deleter;

  void doDelete() {
    try {
      deleter(object);
    } catch(...) {
    }
  }
};

template<typename T, typename U>
void delete_deleter(void* o) {
  // 多重継承の場合でも正しいアドレスが得られるように、
  // void* → T* → U* と2段階に static_cast している。
  delete static_cast<U*>(static_cast<T*>(o));
}

typedef const void* hp_t;

struct HazardBucket {
  std::array<hp_t, HAZARD_BUCKET_SIZE> hp;
  HazardBucket* next;
  int active;

  HazardBucket() : hp(), active(1) {
  }
} __attribute__((aligned(CACHE_LINE_SIZE)));

typedef std::vector<HazardBucket*> HazardBuckets;
typedef std::vector<RetiredItem> RetiredItems;
typedef std::vector<const void*> ScanedSet;

struct HazardRecord {
  HazardRecord* next;
  std::size_t buckets_in_use;
  HazardBuckets hp_buckets;
  RetiredItems retired;
  ScanedSet scaned;
  int active;

  HazardRecord();

  static HazardRecord& getLocalRecord(std::size_t num_buckets);

  static void clearLocalRecord();

  hp_t* getHp(std::size_t start_bucket, std::size_t offset) {
    return &hp_buckets[start_bucket + offset / HAZARD_BUCKET_SIZE]->
        hp[offset % HAZARD_BUCKET_SIZE];
  }

  void addRetired(void* obj, deleter_func del);

} __attribute__((aligned(CACHE_LINE_SIZE)));

}

template<typename T>
class hazard_ptr;

/**
 * hazard_ptr が使用する領域を確保するクラス。
 * このクラスは各スレッドに密接に紐づいているので、
 * 各スレッドのローカル変数としてのみ使用されなければならない。
 * テンプレート引数には、生成する hazard_ptr の個数を指定する。
 */
template<int N>
class hazard_group {
 public:
  hazard_group()
  : hazard_record_(detail::HazardRecord::getLocalRecord(numBuckets())),
    hp_created_(0) {
    if (N > 0) {
      start_bucket_ = hazard_record_.buckets_in_use;
      hazard_record_.buckets_in_use = start_bucket_ + numBuckets();
    }
  }

  hazard_group(const hazard_group&) = delete;
  hazard_group& operator=(const hazard_group&) = delete;

  ~hazard_group() {
    if (N > 0) {
      assert(hazard_record_.buckets_in_use == start_bucket_ + numBuckets());
      hazard_record_.buckets_in_use = start_bucket_;
    }
  }

 private:
  static std::size_t numBuckets() {
    return (N + HAZARD_BUCKET_SIZE - 1) / HAZARD_BUCKET_SIZE;
  }

  detail::hp_t* nextHp() {
    assert(hp_created_ < N);
    return hazard_record_.getHp(start_bucket_, hp_created_++);
  }

  detail::HazardRecord& hazard_record_;
  std::size_t start_bucket_;
  int hp_created_;

  template<typename T>
  friend class hazard_ptr;
};

/**
 * ハザードポインタクラス。
 * ハザードポインタは各スレッドに密接に紐づいているので、
 * 各スレッドのローカル変数としてのみ使用されなければならない。
 */
template<typename T>
class hazard_ptr {
 public:
  typedef T* pointer;
  typedef T* atomic_pointer;
  typedef T element_type;

  /**
   * 新しい hazard_ptr インスタンスを生成する。
   * 一つの hazard_group インスタンスに対しては、そのテンプレート引数の
   * N 個だけ hazard_ptr インスタンスを生成することができる。
   * 生成された hazard_ptr は、元の hazard_group インスタンスの
   * 生存期間でのみ使用することができる。
   */
  template<int N>
  explicit hazard_ptr(hazard_group<N>& hg)
      : hazard_record_(hg.hazard_record_), hp_(hg.nextHp()), ptr_(nullptr) {
  }

  hazard_ptr(const hazard_ptr&) = delete;
  hazard_ptr& operator=(const hazard_ptr&) = delete;

  ~hazard_ptr() {
    reset();
  }

  /**
   * アトミックなポインタ変数から値を読み出し、このハザードポインタに格納する。
   * 読み出したポインタ値が指すオブジェクトは、他スレッドがそのオブジェクトに
   * 対して retire() を呼び出したとしても、
   * このハザードポインタに格納されている間は破棄されない。
   * アトミックポインタ変数からの読み出しにおいては memory_order_acquire 相当の
   * メモリバリア効果を持つ。
   */
  hazard_ptr& load_from(const atomic_pointer* obj) {
    T* p1 = atomic::atomic_load_relaxed(obj);
    for (;;) {
      atomic::atomic_store_release(hp_, static_cast<const void*>(p1));
      atomic::atomic_fence_seq_cst();
      T* p2 = atomic::atomic_load_acquire(obj);
      if (p1 == p2) {
        ptr_ = p2;
        return *this;
      }
      p1 = p2;
    }
  }

  /**
   * 任意のポインタ値をこのハザードポインタに格納する。
   * ただし、ポインタの指すオブジェクトがまだ有効であることを、
   * このメソッドの直後で再確認する必要がある。
   * ある hazard_ptr が保持しているポインタ値を別の hazard_ptr に受け渡す目的で
   * このメソッドを使用してはならない。代わりに swap() を使用すること。
   */
  void reset(T* p) {
    atomic::atomic_store_release(hp_, static_cast<const void*>(p));
    atomic::atomic_fence_seq_cst();
    ptr_ = p;
  }

  /**
   * このハザードポインタが保持しているポインタ値をクリアする。
   */
  void reset(std::nullptr_t) {
    reset();
  }

  /**
   * このハザードポインタが保持しているポインタ値をクリアする。
   */
  void reset() {
    reset_without_fence(nullptr);
  }

  /**
   * 任意のポインタ値をこのハザードポインタに格納する。
   * このメソッドは seq_cst メモリバリアを発行しないので、
   * C++0xのメモリモデルについて正しく理解しており、seq_cst メモリバリア無しでも
   * 安全であると確信している場合以外では使用してはならない。
   */
  void reset_without_fence(T* p) {
    atomic::atomic_store_release(hp_, static_cast<const void*>(p));
    ptr_ = p;
  }

  /**
   * リンクリストの先頭を保持するダミーノードのように、 retire() メソッドで
   * 破棄されることが絶対にないオブジェクトへのポインタをセットする。
   * データ構造を走査する際の初期状態をセットする場合などに用いる。
   */
  void reset_dummy_pointer(T* p) {
    reset();
    ptr_ = p;
  }

  /**
   * このハザードポインタが保持しているオブジェクトを破棄する。
   * ただし、同じオブジェクトへのポインタを他のスレッドのハザードポインタが
   * 保持している間は、破棄が保留される。
   * 破棄するオブジェクトへのポインタは、各スレッドのローカル変数以外の
   * 場所からは消去されていなければならない。
   * オブジェクトへのポインタは、テンプレート引数の型 U に static_cast
   * されてから delete される。
   */
  template<typename U = T>
  void retire() {
    typedef typename std::remove_cv<T>::type TT;
    typedef typename std::remove_cv<U>::type UU;
    TT* obj = const_cast<TT*>(ptr_);
    reset();
    hazard_record_.addRetired(obj, detail::delete_deleter<TT, UU>);
  }

  /**
   * 他のハザードポインタと、保持しているポインタの値を交換する。
   * 交換相手のハザードポインタは、同じ hazard_group から生成されたもので
   * なければならない。
   */
  void swap(hazard_ptr& h) {
    detail::hp_t* tmp_hp = h.hp_;
    T* tmp_ptr = h.ptr_;
    h.hp_ = hp_;
    h.ptr_ = ptr_;
    hp_ = tmp_hp;
    ptr_ = tmp_ptr;
  }

  T& operator*() const {
    return *ptr_;
  }

  T* operator->() const {
    return ptr_;
  }

  T* get() const {
    return ptr_;
  }

  explicit operator bool() const {
    return ptr_ != nullptr;
  }

 private:
  detail::HazardRecord& hazard_record_;
  detail::hp_t* hp_;
  T* ptr_;
};

/**
 * ハザードポインタが使用するスレッドローカルな環境を管理するためのクラス。
 * C++0xの Thread-local storage が正式にサポートされれば、
 * このクラスは不要になるはず……
 */
class hazard_context {
 public:
  hazard_context() = default;
  hazard_context(const hazard_context&) = delete;
  hazard_context& operator=(const hazard_context&) = delete;

  ~hazard_context() {
    detail::HazardRecord::clearLocalRecord();
  }
};

}

#endif
