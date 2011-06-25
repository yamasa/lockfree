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

// スレッド毎のハザードポインタの最大数。
#ifndef HAZARD_PTR_SIZE
#define HAZARD_PTR_SIZE 3
#endif

#ifndef CACHE_LINE_SIZE
#define CACHE_LINE_SIZE 64
#endif

namespace hazard {
namespace detail {

typedef void (*deleter_func)(void*, void*);

struct RetiredItem {
  void* object;
  void* allocator;
  deleter_func deleter;

  void doDelete() {
    deleter(object, allocator);
  }
};

template<typename T, typename U>
void delete_deleter(void* o, void* a) {
  // 多重継承の場合でも正しいアドレスが得られるように、
  // void* → T* → U* と2段階に static_cast している。
  delete static_cast<U*>(static_cast<T*>(o));
}

template<typename T, typename Alloc>
void allocator_deleter(void* o, void* a) {
  Alloc* alloc = static_cast<Alloc*>(a);
  typedef typename Alloc::pointer pointer;
  pointer obj = static_cast<pointer>(static_cast<T*>(o));
  alloc->destroy(obj);
  alloc->deallocate(obj, 1);
}

typedef std::vector<RetiredItem> RetiredItems;
typedef const void* hp_t;

struct HazardRecord {
  HazardRecord* next;
  std::array<hp_t, HAZARD_PTR_SIZE> hp;
  RetiredItems retired;
  int active;

  HazardRecord() : hp(), active(1) {
  }
} __attribute__((aligned(CACHE_LINE_SIZE)));

class LocalData {
 public:
  LocalData();

  LocalData(const LocalData&) = delete;
  LocalData& operator=(const LocalData&) = delete;

  ~LocalData();

  static LocalData& getLocalData();

  hp_t* allocateHpArray(std::size_t n) {
    hp_t* array = &hazard_record_->hp[hp_used_];
    hp_used_ += n;
    assert(hp_used_ <= hazard_record_->hp.size());
    return array;
  }

  void deallocateHpArray(hp_t* array, std::size_t n) {
    hp_used_ -= n;
    assert(array == &hazard_record_->hp[hp_used_]);
  }

  void addRetired(void* obj, void* alloc, deleter_func del);

 private:
  HazardRecord* const hazard_record_;
  std::size_t hp_used_;
};

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
class hazard_array {
 public:
  hazard_array() : local_data_(detail::LocalData::getLocalData()),
                   array_(local_data_.allocateHpArray(N)), num_hp_(0) {
  }

  hazard_array(const hazard_array&) = delete;
  hazard_array& operator=(const hazard_array&) = delete;

  ~hazard_array() {
    atomic::atomic_fence_release();
    for (int i = 0; i < N; i++)
      atomic::atomic_store_relaxed(array_ + i,
                                   static_cast<const void*>(nullptr));
    local_data_.deallocateHpArray(array_, N);
  }

 private:
  detail::LocalData& local_data_;
  detail::hp_t* const array_;
  int num_hp_;

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
   * 一つの hazard_array インスタンスに対しては、そのテンプレート引数の
   * N 個だけ hazard_ptr インスタンスを生成することができる。
   * 生成された hazard_ptr は、元の hazard_array インスタンスの
   * 生存期間でのみ使用することができる。
   */
  template<int N>
  explicit hazard_ptr(hazard_array<N>& ha)
      : local_data_(ha.local_data_), hp_(ha.array_ + ha.num_hp_++),
        ptr_(nullptr) {
    assert(ha.num_hp_ <= N);
  }

  hazard_ptr(const hazard_ptr&) = delete;
  hazard_ptr& operator=(const hazard_ptr&) = delete;

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
    local_data_.addRetired(obj, nullptr, detail::delete_deleter<TT, UU>);
  }

  /**
   * 引数無しの retire() と同様だが、オブジェクトの破棄は引数のアロケータに
   * よって行なわれる。
   * 注意すべき点として、オブジェクトの破棄は長期間保留されることがあるので、
   * アロケータの寿命は少なくともハザードポインタを使用する各スレッドよりも
   * 長くなくてはならない。
   */
  template<typename Alloc>
  void retire(Alloc* alloc) {
    typedef typename std::remove_cv<T>::type TT;
    TT* obj = const_cast<TT*>(ptr_);
    reset();
    local_data_.addRetired(obj, alloc, detail::allocator_deleter<TT, Alloc>);
  }

  /**
   * 他のハザードポインタと、保持しているポインタの値を交換する。
   * 交換相手のハザードポインタは、同じ hazard_array から生成されたもので
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
  detail::LocalData& local_data_;
  detail::hp_t* hp_;
  T* ptr_;
};

/**
 * ハザードポインタが動作するために必要なスレッドローカルな環境を設定するための
 * クラス。ハザードポインタを利用するスレッドの先頭でこのインスタンスを生成して
 * おく必要がある。
 * C++0xの Thread-local storage が正式にサポートされれば、
 * このクラスは不要になるはず……
 */
class hazard_context {
 public:
  hazard_context() : local_data_() {
  }

  hazard_context(const hazard_context&) = delete;
  hazard_context& operator=(const hazard_context&) = delete;

 private:
  detail::LocalData local_data_;
};

}

#endif
