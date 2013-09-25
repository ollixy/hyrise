#ifdef PERSISTENCY_NVRAM

// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "testing/test.h"
#include "storage/NVVector.h"

#include <algorithm>

namespace hyrise {
namespace storage {

class NVVectorTests : public ::hyrise::Test {
};

TEST_F(NVVectorTests, basic_test) {
  NVVector<int> v(100);
  ASSERT_EQ(100u, v.size());
  for(int i = 0; i < 100; i++) ASSERT_EQ(0, v[i]) << "w/ i=" << i;

  for(int i = 0; i < 100; i++) v[i] = 200 + i;
  for(int i = 0; i < 100; i++) ASSERT_EQ(200 + i, v[i]) << "w/ i=" << i;

  io::NVManager::getInstance().verifyCanaries();
}

TEST_F(NVVectorTests, construct_w_value_test) {
  NVVector<int> v(10, 5);
  ASSERT_EQ(10u, v.size());
  ASSERT_EQ(5, v[0]);
  ASSERT_EQ(5, v[3]);

  io::NVManager::getInstance().verifyCanaries();
}

TEST_F(NVVectorTests, restore_test) {
  NVVector<int> v2(100);
  v2[0] = 123;
  v2[1] = 2;

  auto vi = io::NVManager::getInstance().getOrCreateVectorSpace(v2.getUUID(), 0);
  NVVector<int> v(vi);
  ASSERT_EQ(123, v[0]);
  ASSERT_EQ(2, v[1]);
  ASSERT_EQ(100u, v.size());

  io::NVManager::getInstance().verifyCanaries();
}

TEST_F(NVVectorTests, reserve_test) {
  NVVector<int> v(10);
  ASSERT_EQ(10u, v.size());
  ASSERT_EQ(10u, v.capacity());
  v.reserve(20);
  ASSERT_EQ(10u, v.size());
  ASSERT_EQ(20u, v.capacity());

  io::NVManager::getInstance().verifyCanaries();
}

TEST_F(NVVectorTests, resize_test) {
  NVVector<int> v(20);
  for(int i = 0; i < 20; i++) v[i] = 200 + i;

  v.resize(30);
  ASSERT_EQ(30u, v.size());
  for(int i = 0; i < 20; i++) ASSERT_EQ(200 + i, v[i]) << "w/ i=" << i;
  for(int i = 20; i < 30; i++) ASSERT_EQ(0, v[i]) << "w/ i=" << i;

  v.resize(40, 51);
  ASSERT_EQ(40u, v.size());
  for(int i = 0; i < 20; i++) ASSERT_EQ(200 + i, v[i]) << "w/ i=" << i;
  for(int i = 20; i < 30; i++) ASSERT_EQ(0, v[i]) << "w/ i=" << i;
  for(int i = 30; i < 40; i++) ASSERT_EQ(51, v[i]) << "w/ i=" << i;

  v.reserve(50);

  v.resize(10);
  ASSERT_EQ(10u, v.size());
  for(int i = 0; i < 10; i++) ASSERT_EQ(200 + i, v[i]) << "w/ i=" << i;

  io::NVManager::getInstance().verifyCanaries();
}

TEST_F(NVVectorTests, assign_test) {
  NVVector<int> v(20);
  v[0] = 123;
  v[1] = 2;

  v.assign(10, 2);
  ASSERT_EQ(10u, v.size());
  ASSERT_EQ(2, v[0]);
  ASSERT_EQ(2, v[1]);
  ASSERT_EQ(2, v[2]);

  v.assign(30, 5);
  ASSERT_EQ(30u, v.size());
  ASSERT_EQ(5, v[0]);
  ASSERT_EQ(5, v[1]);
  ASSERT_EQ(5, v[2]);
  ASSERT_EQ(5, v[15]);

  io::NVManager::getInstance().verifyCanaries();
}

TEST_F(NVVectorTests, stl_sort_compatibility_test) {
  NVVector<int> v;
  for(int i=0; i<100; ++i)
    v.push_back(i % 10);
  std::sort(v.begin(), v.end());
  ASSERT_EQ(true, std::is_sorted(v.begin(), v.end()));

  io::NVManager::getInstance().verifyCanaries();
}

TEST_F(NVVectorTests, overflow_test) {
  NVVector<int> v1(100, 1), v2(100, 2);
  for(int i = 0; i < 100; i++) ASSERT_EQ(1, v1[i]) << "w/ i=" << i;
  for(int i = 0; i < 100; i++) ASSERT_EQ(2, v2[i]) << "w/ i=" << i;

  v1.resize(105, 3);
  for(int i = 0; i < 100; i++) ASSERT_EQ(1, v1[i]) << "w/ i=" << i;
  for(int i = 100; i < 105; i++) ASSERT_EQ(3, v1[i]) << "w/ i=" << i;
  for(int i = 0; i < 100; i++) ASSERT_EQ(2, v2[i]) << "w/ i=" << i;

  v2.assign(105, 4);
  for(int i = 0; i < 100; i++) ASSERT_EQ(1, v1[i]) << "w/ i=" << i;
  for(int i = 100; i < 105; i++) ASSERT_EQ(3, v1[i]) << "w/ i=" << i;
  for(int i = 0; i < 105; i++) ASSERT_EQ(4, v2[i]) << "w/ i=" << i;

  io::NVManager::getInstance().verifyCanaries();
}

}
}

#endif

