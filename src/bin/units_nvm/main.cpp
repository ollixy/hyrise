// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "testing/main.h"
#ifdef PERSISTENCY_NVRAM
#include "io/NVManager.h"
#endif

int main(int argc, char **argv) {
#ifdef PERSISTENCY_NVRAM
  ::hyrise::io::NVManager::setNonVolatileMode();
  ::hyrise::testing::minimalistMain(argc, argv);
 #endif
}
