// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "io/shortcuts.h"
#include "testing/test.h"

#include "helper.h"
#include <iostream>
#include <fstream>

namespace {
  const std::string tpccQueryPath = "test/json/tpcc-queries";
}

namespace hyrise {
namespace access {

class TpccExecuteTest : public AccessTest {
public:
  TpccExecuteTest() {
    _deliveryMap["deleteNewOrder"]  = tpccQueryPath + "/Delivery-deleteNewOrder.json";
    _deliveryMap["getCId"]          = tpccQueryPath + "/Delivery-getCId.json";
    _deliveryMap["getNewOrder"]     = tpccQueryPath + "/Delivery-getNewOrder.json";
    _deliveryMap["sumOLAmount"]     = tpccQueryPath + "/Delivery-sumOLAmount.json";
    _deliveryMap["updateCustomer"]  = tpccQueryPath + "/Delivery-updateCustomer.json";
    _deliveryMap["updateOrderLine"] = tpccQueryPath + "/Delivery-updateOrderLine.json";
    _deliveryMap["updateOrders"]    = tpccQueryPath + "/Delivery-updateOrders.json";

    _newOrderMap["createNewOrder"]       = tpccQueryPath + "/NewOrder-createNewOrder.json";
    _newOrderMap["createOrder"]          = tpccQueryPath + "/NewOrder-createOrder.json";
    _newOrderMap["getCustomer"]          = tpccQueryPath + "/NewOrder-getCustomer.json";
    _newOrderMap["getDistrict"]          = tpccQueryPath + "/NewOrder-getDistrict.json";
    _newOrderMap["getItemInfo"]          = tpccQueryPath + "/NewOrder-getItemInfo.json";
    _newOrderMap["getStockInfo"]         = tpccQueryPath + "/NewOrder-getStockInfo.json";
    _newOrderMap["getWarehouseTaxRate"]  = tpccQueryPath + "/NewOrder-getWarehouseTaxRate.json";
    _newOrderMap["incrementNextOrderId"] = tpccQueryPath + "/NewOrder-incrementNextOrderId.json";
    _newOrderMap["updateStock"]          = tpccQueryPath + "/NewOrder-updateStock.json";

    _orderStatusMap["getCustomerByCId"]       = tpccQueryPath + "/OrderStatus-getCustomerByCId.json";
    _orderStatusMap["getCustomersByLastName"] = tpccQueryPath + "/OrderStatus-getCustomersByLastName.json";
    _orderStatusMap["getLastOrder"]           = tpccQueryPath + "/OrderStatus-getLastOrder.json";
    _orderStatusMap["getOrderLines"]          = tpccQueryPath + "/OrderStatus-getOrderLines.json";

    _paymentMap["getCustomerByCId"]       = tpccQueryPath + "/Payment-getCustomerByCId.json";
    _paymentMap["getCustomersByLastName"] = tpccQueryPath + "/Payment-getCustomersByLastName.json";
    _paymentMap["getDistrict"]            = tpccQueryPath + "/Payment-getDistrict.json";
    _paymentMap["getWarehouse"]           = tpccQueryPath + "/Payment-getWarehouse.json";
    _paymentMap["insertHistory"]          = tpccQueryPath + "/Payment-insertHistory.json";
    _paymentMap["updateBCCustomer"]       = tpccQueryPath + "/Payment-updateBCCustomer.json";
    _paymentMap["updateDistrictBalance"]  = tpccQueryPath + "/Payment-updateDistrictBalance.json";
    _paymentMap["updateGCCustomer"]       = tpccQueryPath + "/Payment-updateGCCustomer.json";
    _paymentMap["updateWarehouseBalance"] = tpccQueryPath + "/Payment-updateWarehouseBalance.json";

    _stockLevelMap["getOId"]        = tpccQueryPath + "/StockLevel-getOId.json";
    _stockLevelMap["getStockCount"] = tpccQueryPath + "/StockLevel-getStockCount.json";
  }

protected:
  typedef std::map<std::string, std::string> file_map_t;

  void assureQueryFilesExist(const file_map_t& map) {
    for (auto& value : map) {
      std::ifstream s(value.second);
      if (!s)
        throw std::runtime_error("cannot read from file \"" + value.second + "\"");
      s.close();
    }
  }

  file_map_t _deliveryMap, _newOrderMap, _orderStatusMap, _paymentMap, _stockLevelMap;
};

TEST_F(TpccExecuteTest, DeliveryTest) {
  std::cout << "asdfasdfalsdjkfasjkdfasdkf" << std::endl << std::endl;
  assureQueryFilesExist(_deliveryMap);

  parameter_map_t map;
  addParameteri(map, "w_id", 1);
  addParameteri(map, "d_id", 1);
  addParameters(map, "o_carrier_id", "???");
  addParameteri(map, "ol_delivery_d", 100.34);
  
  // getNewOrder
  auto t1 = executeAndWait(loadParameterized(_deliveryMap["getNewOrder"], map));
  ASSERT_GE(t1->size(), 1);
  const int no_o_id = t1->getValue<int>(0, 0);
  addParameteri(map, "no_o_id", no_o_id);

  // getCId
  auto t2 = executeAndWait(loadParameterized(_deliveryMap["getCId"], map));
  ASSERT_GE(t2->size(), 1);
  const int c_id = t2->getValue<int>(0, 0);
  addParameteri(map, "c_id", c_id);

  auto t3 = executeAndWait(loadParameterized(_deliveryMap["sumOLAmount"], map));
  ASSERT_EQ(1, t3->size());
  ASSERT_EQ(1, t3->columnCount());
  const int ol_total = t3->getValue<int>(0, 0);
  addParameteri(map, "ol_total", ol_total);

  std::cout << ">>" << loadParameterized(_deliveryMap["deleteNewOrder"], map) << std::endl;
  //executeAndWait(loadParameterized(_deliveryMap["deleteNewOrder"], map));

  std::cout << ">>" << loadParameterized(_deliveryMap["updateOrders"], map) << std::endl;
  //executeAndWait(loadParameterized(_deliveryMap["updateOrders"], map));

  std::cout << ">>" << loadParameterized(_deliveryMap["updateOrderLine"], map) << std::endl;
  //executeAndWait(loadParameterized(_deliveryMap["updateOrderLine"], map));

  std::cout << ">>" << loadParameterized(_deliveryMap["updateCustomer"], map) << std::endl;
  //executeAndWait(loadParameterized(_deliveryMap["updateCustomer"], map));
}

TEST_F(TpccExecuteTest, NewOrderTest) {
  assureQueryFilesExist(_newOrderMap);

  parameter_map_t map;
  addParameteri(map, "w_id", 1);
  addParameteri(map, "d_id", 1);
  addParameteri(map, "c_id", 1);
  addParameteri(map, "o_entry_d", 1);
  addParameteri(map, "i_ids", 1);
  addParameteri(map, "i_w_ids", 1);
  addParameteri(map, "i_qtys", 1);

}

TEST_F(TpccExecuteTest, OrderStatusTest) {
  assureQueryFilesExist(_orderStatusMap);

  parameter_map_t map;
  addParameteri(map, "w_id", 1);
  addParameteri(map, "d_id", 1);
  addParameteri(map, "c_id", 1);
  addParameters(map, "c_last", "????");

}

TEST_F(TpccExecuteTest, PaymentTest) {
  assureQueryFilesExist(_paymentMap);

  parameter_map_t map;
  addParameteri(map, "w_id", 1);
  addParameteri(map, "d_id", 1);
  addParameterf(map, "h_amout", 1);
  addParameteri(map, "c_w_id", 1);
  addParameteri(map, "c_d_id", 1);
  addParameteri(map, "c_id", 1);
  addParameters(map, "c_last", "?????");
  addParameters(map, "h_date", "00-00-0000");
}

TEST_F(TpccExecuteTest, StockLevelTest) {
  assureQueryFilesExist(_stockLevelMap);

  parameter_map_t map;
  addParameteri(map, "w_id", 1);
  addParameteri(map, "d_id", 1);
  addParameteri(map, "threshhold", 1);
}

} } // namespace hyrise::access

