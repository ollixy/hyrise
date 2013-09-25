// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#ifndef SRC_LIB_STORAGE_FIXEDLENGTHVECTOR_H_
#define SRC_LIB_STORAGE_FIXEDLENGTHVECTOR_H_

#include "storage/BaseAttributeVector.h"

template <typename T>
class FixedLengthVector : public BaseAttributeVector<T> {
 public:  
  virtual void *data() = 0;

  virtual void setNumRows(size_t s) = 0;

  virtual T get(size_t column, size_t row) const = 0;

  virtual const T& getRef(size_t column, size_t row) const = 0;

  virtual void set(size_t column, size_t row, T value) = 0;

  virtual void reserve(size_t rows) = 0;

  virtual void clear() = 0;

  virtual size_t size() = 0;

  virtual void resize(size_t rows) = 0;

  virtual void rewriteColumn(const size_t column, const size_t bits) = 0;

  // returns the capacity of the container
  virtual uint64_t capacity() = 0;

  virtual std::shared_ptr<BaseAttributeVector<T>> copy() = 0;

  // Increment the value by 1
  virtual T inc(size_t column, size_t row) = 0;

  // Atomic Increment the value by one
  virtual T atomic_inc(size_t column, size_t row) = 0;

  virtual const std::string print() = 0;
};

#endif  // SRC_LIB_STORAGE_FIXEDLENGTHVECTOR_H_
