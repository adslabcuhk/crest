#include <cstdint>
#include <cstdio>
#include <iostream>

template <typename T>
struct Updater {
  virtual T Op(const T &) = 0;
};

struct VHandle;

template <typename T>
struct VImpl {
  VHandle *vhd;
  Updater<T> *updater;
  bool evaluated;
  T result;

  VImpl(VHandle *vhd) : vhd(vhd), updater(nullptr), evaluated(false), result() {}

  T Access() const;

  T Evaluate();

  void SetUpdater(Updater<T> *updater) { this->updater = updater; }
};

struct VHandle {
  char *d;
  void *impl;

  VHandle() = default;
  ~VHandle() {
    if (impl) {
      // Assuming impl is of type VImpl<uint16_t>
      delete reinterpret_cast<VImpl<uint16_t> *>(impl);  
    }
  }

  template <typename T>
  T Access() const {
    return reinterpret_cast<VImpl<T> *>(impl)->Access();
  }

  template <typename T>
  T Evaluate() const {
    return reinterpret_cast<VImpl<T> *>(impl)->Evaluate();
  }

  template <typename T>
  void SetUpdater(Updater<T> *updater) {
    reinterpret_cast<VImpl<T> *>(impl)->SetUpdater(updater);
  }
};

template <typename T>
T VImpl<T>::Access() const {
  return *reinterpret_cast<T *>(vhd->d);
}

template <typename T>
T VImpl<T>::Evaluate() {
  if (!evaluated) {
    result = updater->Op(Access());
    evaluated = true;
  }
  return result;
}

struct WarehouseValue {
  uint16_t w_id;
  uint32_t d_id;
  uint64_t c_id;
  char address[128];

 public:
  VHandle *GetVHandle(int cid) {
    VHandle *ret = &vhds[cid];
    switch (cid) {
      case 0:
        ret->d = (char *)(&w_id);
        ret->impl = new VImpl<uint16_t>(ret);
        return ret;
      case 1:
        ret->d = (char *)(&d_id);
        ret->impl = new VImpl<uint32_t>(ret);
        return ret;
      case 2:
        ret->d = (char *)(&c_id);
        ret->impl = new VImpl<uint64_t>(ret);
        return ret;
    }
    return nullptr;
  }

 private:
  VHandle vhds[3];
};

void TxUpdateDid() {
  WarehouseValue w_val;

  w_val.w_id = 10;
  w_val.d_id = 20;
  w_val.c_id = 55;

  VHandle *wid_hdl = w_val.GetVHandle(0);
  VHandle *did_hdl = w_val.GetVHandle(1);

  struct WarehouseDidUpdater : public Updater<uint32_t> {
    WarehouseDidUpdater(VHandle *wid_handle) : wid_handle(wid_handle) {}

   public:
    uint32_t Op(const uint32_t &n) {
      uint16_t w_id_val = wid_handle->Access<uint16_t>();
      return w_id_val + n * 2;
    }

   private:
    VHandle *wid_handle;
  } updater(wid_hdl);

  did_hdl->SetUpdater(&updater);

  auto d_id_value = did_hdl->Access<uint32_t>();
  auto d_id_new_value = did_hdl->Evaluate<uint32_t>();

  std::cout << "d_id_value: " << d_id_value << "\n"
            << "d_id_new_value: " << d_id_new_value << '\n';
};

int main() { TxUpdateDid(); }