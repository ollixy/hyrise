// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "io/shortcuts.h"
#include "testing/test.h"

#include "helper.h"
#include <iostream>
#include <fstream>

#include <io/ResourceManager.h>
#include <storage/TableDiff.h>
#include <storage/PrettyPrinter.h>

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
    _newOrderMap["createOrderLine"]      = tpccQueryPath + "/NewOrder-createOrderLine.json";
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

  void TearDown() {
    io::ResourceManager::getInstance().clear();
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
    std::string tableName;
    std::string fileName;
    switch (table) {
      case Customer:  tableName = "CUSTOMER"; fileName = "customer"; break;
      case Orders:    tableName = "ORDERS"; fileName = "order"; break;
      case OrderLine: tableName = "ORDER-LINE"; fileName = "order_line"; break;
      case Warehouse: tableName = "WAREHOUSE"; fileName = "warehouse"; break;
      case NewOrder:  tableName = "NEW-ORDER"; fileName = "new_order"; break;
      case District:  tableName = "DISTRICT"; fileName = "district"; break;
      case Item:      tableName = "ITEM"; fileName = "item"; break;
      case Stock:     tableName = "STOCK"; fileName = "stock"; break;
      case History:   tableName = "HISTORY"; fileName = "history"; break;
    }
    return executeAndWait("{\"operators\": {\"load\": {\"type\": \"TableLoad\", \"filename\": \"tables/tpcc/" + fileName + ".tbl\", \"table\": \"" + tableName + "\"}" +
                          ", \"validate\": {\"type\": \"ValidatePositions\"}}, \"edges\": [[\"load\", \"validate\"]]}", tid);
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
  setParameteri(map, "w_id", 1);
  setParameteri(map, "d_id", 1);
  setParameteri(map, "o_carrier_id", 1337);
  setParameters(map, "ol_delivery_d", "2013-09-20-02-16-31");
  
  tx::transaction_id_t tid = tx::UNKNOWN;
  
  // getNewOrder
  auto t1 = executeAndWait(loadParameterized(_deliveryMap["getNewOrder"], map), &tid);
  ASSERT_GE(t1->size(), 1);
  const int no_o_id = t1->getValue<int>("NO_O_ID", 0);
  setParameteri(map, "no_o_id", no_o_id);

  // getCId
  auto t2 = executeAndWait(loadParameterized(_deliveryMap["getCId"], map), &tid);
  ASSERT_EQ(t2->size(), 1);
  const int c_id = t2->getValue<int>("O_C_ID", 0);
  setParameteri(map, "c_id", c_id);

  //sumOLAmount
  auto t3 = executeAndWait(loadParameterized(_deliveryMap["sumOLAmount"], map), &tid);
  ASSERT_EQ(1, t3->size());
  ASSERT_EQ(1, t3->columnCount());
  const float ol_total = t3->getValue<float>(0, 0);
  setParameterf(map, "ol_total", ol_total);

  executeAndWait(loadParameterized(_deliveryMap["deleteNewOrder"], map), &tid); 
  executeAndWait(loadParameterized(_deliveryMap["updateOrders"], map), &tid);
  executeAndWait(loadParameterized(_deliveryMap["updateOrderLine"], map), &tid);
  executeAndWait(loadParameterized(_deliveryMap["updateCustomer"], map), &tid);
  executeAndWait(_commit, &tid);

  //updates
  EXPECT_EQ(i_orders_size, loadTable(Orders)->size()) << "number of rows in ORDERS should not change";
  EXPECT_EQ(i_orderLine_size, loadTable(OrderLine)->size()) << "number of rows in ORDER-LINE should not change";
  EXPECT_EQ(i_customer_size, loadTable(Customer)->size()) << "number of rows in CUSTOMER should not change";

  //deletes
  EXPECT_EQ(i_newOrder_size, loadTable(NewOrder)->size() + 1) << "number of rows in New-Order should be exactly one less";
}

TEST_F(TpccExecuteTest, NewOrderTest) {
  unsigned ol_cnt = 5;
  unsigned all_local = 1;
  bool rollback = false;
  std::vector<int> i_ids(ol_cnt, 2);

  assureQueryFilesExist(_newOrderMap);

  parameter_map_t map;
  setParameteri(map, "w_id", 1);
  setParameteri(map, "d_id", 1);
  setParameteri(map, "c_id", 1);
  setParameteri(map, "c_id", 2);
  setParameters(map, "o_entry_d", "2013-09-20-02-16-31");
  setParameteri(map, "o_carrier_id", 1337);
  setParameteri(map, "o_ol_cnt", ol_cnt);
  //setParameteri(map, "i_qtys", 1);
  setParameteri(map, "all_local", all_local);

  tx::transaction_id_t tid = tx::UNKNOWN;

  typedef struct item_info_t { float price; std::string name; std::string data; } ItemInfo;
  std::vector<ItemInfo> items;
  items.resize(ol_cnt);

// getNewOrder
  for (unsigned i = 0; i < ol_cnt; ++i) {
    setParameteri(map, "i_id", i_ids.at(i));
    auto t1 = executeAndWait(loadParameterized(_newOrderMap["getItemInfo"], map), &tid);
    ASSERT_EQ(t1->size(), 1);
    items[i].price = t1->getValue<float>("I_PRICE", 0);
    items[i].name = t1->getValue<std::string>("I_NAME", 0);
    items[i].data = t1->getValue<std::string>("I_DATA", 0);
  }

  auto t2 = executeAndWait(loadParameterized(_newOrderMap["getWarehouseTaxRate"], map), &tid);
  ASSERT_EQ(t2->size(), 1);
  const float w_tax = t2->getValue<float>("W_TAX", 0);

  auto t3 = executeAndWait(loadParameterized(_newOrderMap["getDistrict"], map), &tid);
  ASSERT_EQ(t3->size(), 1);
  const float d_tax = t3->getValue<float>("D_TAX", 0);
  const unsigned o_id = t3->getValue<int>("D_NEXT_O_ID", 0);
  setParameteri(map, "o_id", o_id);

  auto t4 = executeAndWait(loadParameterized(_newOrderMap["getCustomer"], map), &tid);
  ASSERT_EQ(t4->size(), 1);
  const float c_discount = t4->getValue<float>("C_DISCOUNT", 0);
  const std::string c_last = t4->getValue<std::string>("C_LAST", 0);
  const std::string c_credit = t4->getValue<std::string>("C_CREDIT", 0);

  setParameteri(map, "d_next_o_id", o_id + 1);
  executeAndWait(loadParameterized(_newOrderMap["incrementNextOrderId"], map), &tid);
  std::cout << loadParameterized(_newOrderMap["createOrder"], map);
  executeAndWait(loadParameterized(_newOrderMap["createOrder"], map), &tid); //ERROR!
  executeAndWait(loadParameterized(_newOrderMap["createNewOrder"], map), &tid);

  auto t5 = executeAndWait(loadParameterized(_newOrderMap["getStockInfo"], map), &tid);
  t5->print();
  ASSERT_EQ(t5->size(), 1);
  const unsigned s_quantity = t5->getValue<int>("S_QUANTITY", 0);
  const int s_ytd = t5->getValue<int>("S_YTD", 0);
  const unsigned s_order_cnt = t5->getValue<int>("S_ORDER_CNT", 0);
  const unsigned s_remote_cnt = t5->getValue<int>("S_REMOTE_CNT", 0);
  const std::string s_data = t5->getValue<std::string>("S_DATA", 0);
  ASSERT_EQ(t5->columnCount(), 6);
  const std::string s_dist = t5->getValue<std::string>(5, 0);
  
  std::cout << loadParameterized(_newOrderMap["updateStock"], map);
  std::cout << loadParameterized(_newOrderMap["createOrderLine"], map);
}

TEST_F(TpccExecuteTest, OrderStatusTest) {
  bool selectByLastName = true;
  
  assureQueryFilesExist(_orderStatusMap);

  parameter_map_t map;
  setParameteri(map, "w_id", 1);
  setParameteri(map, "d_id", 1);
  setParameteri(map, "c_id", 1);
  setParameters(map, "c_last", "BARBARBAR");

  tx::transaction_id_t tid = tx::UNKNOWN;
  
  //actually only one of them:
  auto t1 = executeAndWait(loadParameterized(_orderStatusMap["getCustomerByCId"], map), &tid);
  ASSERT_EQ(1, t1->size());

  auto t2 = executeAndWait(loadParameterized(_orderStatusMap["getCustomersByLastName"], map), &tid);
  ASSERT_GE(t2->size(), 1);
  const unsigned chosenOne = (t2->size() - 1) / 2;
  setParameteri(map, "c_id", t2->getValue<int>("C_ID", chosenOne));

  auto t3 = executeAndWait(loadParameterized(_orderStatusMap["getLastOrder"], map), &tid);
  ASSERT_GE(1, t3->size());
  const unsigned o_id = t3->getValue<int>("O_ID", 0);
  setParameteri(map, "o_id", o_id);

  auto t4 = executeAndWait(loadParameterized(_orderStatusMap["getOrderLines"], map), &tid);
  ASSERT_GE(1, t4->size());
}

TEST_F(TpccExecuteTest, PaymentTest) {
  assureQueryFilesExist(_paymentMap);

  parameter_map_t map;
  setParameteri(map, "w_id", 1);
  setParameteri(map, "d_id", 1);
  setParameterf(map, "h_amout", 1);
  setParameteri(map, "c_w_id", 1);
  setParameteri(map, "c_d_id", 1);
  setParameteri(map, "c_id", 1);
  setParameters(map, "c_last", "BARBARBAR");
  setParameters(map, "h_date", "2013-09-20-02-16-31");

  tx::transaction_id_t tid = tx::UNKNOWN;
  
}

TEST_F(TpccExecuteTest, StockLevelTest) {
  assureQueryFilesExist(_stockLevelMap);

  parameter_map_t map;
  setParameteri(map, "w_id", 1);
  setParameteri(map, "d_id", 1);
  setParameteri(map, "threshold", 1);

  tx::transaction_id_t tid = tx::UNKNOWN;

  auto t1 = executeAndWait(loadParameterized(_stockLevelMap["getOId"], map), &tid);
  ASSERT_EQ(1, t1->size());
  const unsigned o_id = t1->getValue<int>("D_NEXT_O_ID", 0);
  setParameteri(map, "o_id1", o_id);
  setParameteri(map, "o_id2", o_id-20);

  std::cout << loadParameterized(_stockLevelMap["getStockCount"], map);
  //auto t2 = executeAndWait(loadParameterized(_stockLevelMap["getStockCount"], map), &tid);
}

} } // namespace hyrise::access

