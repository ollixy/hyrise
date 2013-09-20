// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "io/shortcuts.h"
#include "testing/test.h"

#include "helper.h"
#include <iostream>
#include <fstream>

#include <io/ResourceManager.h>

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

  void SetUp() {
    io::ResourceManager::getInstance().clear();
    
    i_customer_size  = loadTable(Customer)->size();
    i_orders_size    = loadTable(Orders)->size();
    i_orderLine_size = loadTable(OrderLine)->size();
    i_warehouse_size = loadTable(Warehouse)->size();
    i_newOrder_size  = loadTable(NewOrder)->size();
    i_district_size  = loadTable(District)->size();
    i_item_size      = loadTable(Item)->size();
    i_stock_size     = loadTable(Stock)->size();
    i_history_size   = loadTable(History)->size();
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
  size_t i_customer_size, i_orders_size, i_orderLine_size, i_warehouse_size, i_newOrder_size,
         i_district_size, i_item_size, i_stock_size, i_history_size;

  const std::string _commit = "{\"operators\": {\"commit\": {\"type\": \"Commit\"}}}";
  const std::string _rollback = "{\"operators\": {\"rollback\": {\"type\": \"Rollback\"}}}";


  enum TpccTable { Customer, Orders, OrderLine, Warehouse, NewOrder, District, Item, Stock, History };

  static storage::c_atable_ptr_t loadTable(const TpccTable table, tx::transaction_id_t* tid = nullptr) {
    std::stringstream buf;
    buf << "{\"operators\":{\"load\": {\"type\": \"TableLoad\", \"filename\": \"tables/tpcc/";
    switch (table) {
      case Customer:  buf << "customer"; break;
      case Orders:    buf << "order"; break;
      case OrderLine: buf << "order_line"; break;
      case Warehouse: buf << "warehouse"; break;
      case NewOrder:  buf << "new_order"; break;
      case District:  buf << "district"; break;
      case Item:      buf << "item"; break;
      case Stock:     buf << "stock"; break;
      case History:   buf << "history"; break;
    }
    buf << ".tbl\"}}}";
    return executeAndWait(buf.str(), tid);
  }
};

TEST_F(TpccExecuteTest, LoadTables) {
  auto customer = loadTable(Customer);
  EXPECT_LT(0, customer->size()) << "invalid customer table";

  auto orders = loadTable(Orders);
  EXPECT_LT(0, orders->size()) << "invalid orders table";
  
  auto orderLine = loadTable(OrderLine);
  EXPECT_LT(0, orderLine->size()) << "invalid order_line table";
  
  auto warehouse = loadTable(Warehouse);
  EXPECT_LT(0, warehouse->size()) << "invalid warehouse table";
  
  auto newOrder = loadTable(NewOrder);
  EXPECT_LT(0, newOrder->size()) << "invalid new_order table";
  
  auto district = loadTable(District);
  EXPECT_LT(0, district->size()) << "invalid district table";
  
  auto item = loadTable(Item);
  EXPECT_LT(0, item->size()) << "invalid item table";
  
  auto stock = loadTable(Stock);
  EXPECT_LT(0, stock->size()) << "invalid stock table";
  
  auto history = loadTable(History);
  EXPECT_LT(0, history->size()) << "invalid history table";
}

TEST_F(TpccExecuteTest, DeliveryTest) {
  assureQueryFilesExist(_deliveryMap);

  parameter_map_t map;
  addParameteri(map, "w_id", 1);
  addParameteri(map, "d_id", 1);
  addParameteri(map, "o_carrier_id", 1337);
  addParameters(map, "ol_delivery_d", "2013-09-20-02-16-31");
  
  tx::transaction_id_t tid = tx::UNKNOWN;

  // getNewOrder
  auto t1 = executeAndWait(loadParameterized(_deliveryMap["getNewOrder"], map), &tid);
  ASSERT_GE(t1->size(), 1);
  const int no_o_id = t1->getValue<int>(0, 0);
  addParameteri(map, "no_o_id", no_o_id);

  // getCId
  auto t2 = executeAndWait(loadParameterized(_deliveryMap["getCId"], map), &tid);
  ASSERT_GE(t2->size(), 1);
  const int c_id = t2->getValue<int>(0, 0);
  addParameteri(map, "c_id", c_id);

  auto t3 = executeAndWait(loadParameterized(_deliveryMap["sumOLAmount"], map), &tid);
  ASSERT_EQ(1, t3->size());
  ASSERT_EQ(1, t3->columnCount());
  const float ol_total = t3->getValue<float>(0, 0);
  addParameterf(map, "ol_total", ol_total);

  executeAndWait(loadParameterized(_deliveryMap["deleteNewOrder"], map), &tid);
  executeAndWait(loadParameterized(_deliveryMap["updateOrders"], map), &tid);
  executeAndWait(loadParameterized(_deliveryMap["updateOrderLine"], map), &tid);
  executeAndWait(loadParameterized(_deliveryMap["updateCustomer"], map), &tid);
  executeAndWait(_commit, &tid);

  ASSERT_EQ(i_orders_size, loadTable(Orders)->size()) << "number of rows in ORDERS should not change";
  ASSERT_EQ(i_orderLine_size, loadTable(OrderLine)->size()) << "number of rows in ORDER-LINE should not change";
  ASSERT_EQ(i_customer_size, loadTable(Customer)->size()) << "number of rows in CUSTOMER should not change";

  ASSERT_EQ(i_newOrder_size, loadTable(NewOrder)->size() + 1) << "number of rows in New-Order should be exactly one less";
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

