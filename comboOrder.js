



const mongoose = require('mongoose');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

// MongoDB Connection
mongoose.connect(
  // 'mongodb://developer:pmq21byndxhte3wul7i5fjav9r4o06kg@beta.mongoserver.ko-tech.in:27017/?authSource=admin&directConnection=true&tls=true',
  'mongodb+srv://CRM-Database-ReadOnly:1cS8EhFkOzKwlVuI@cluster0.okk1w.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0&readPreference=secondaryPreferred',
  {
    useNewUrlParser: true,
    useUnifiedTopology: true,
    dbName: 'CRM-Database',
  }
).then(() => console.log('✅ MongoDB connected'))
 .catch(err => console.error('❌ MongoDB connection error:', err));

// Define Schemas
const orderSchema = new mongoose.Schema({
  order_id: String,
  items: [
    {
      sku: String,
      isCombo: Boolean,
      comboSku: String,
    },
  ],
});

const productSchema = new mongoose.Schema({
  sku: String,
  parentSku: String,
  itemQty: Number,
});

// Models
const Order = mongoose.model('Order', orderSchema);
const Product = mongoose.model('Product', productSchema);

// CSV Writer Setup
const csvWriter = createCsvWriter({
  path: 'combo_items_with_itemQty_gt_22.csv',
  header: [
    { id: 'orderId', title: 'OrderID' },
    { id: 'comboSku', title: 'ComboSKU' },
    { id: 'sku', title: 'SKU' },
    { id: 'parentSku', title: 'ParentSKU' },
    { id: 'itemQty', title: 'ItemQty' },
  ],
});

async function exportOrdersWithChildProductsQtyGT1() {
  try {
    const orders = await Order.find({}, { order_id: 1, items: 1 }).lean();
    const resultRows = [];

    // Collect all unique SKUs from isCombo items
    const comboSkus = [];
    for (const order of orders) {
      for (const item of order.items || []) {
        if (item.isCombo && item.comboSku && item.sku) {
          comboSkus.push(item.sku);
        }
      }
    }

    // Remove duplicates
    const uniqueSkus = [...new Set(comboSkus)];

    // Fetch all products in one go
    const products = await Product.find(
      { sku: { $in: uniqueSkus }, itemQty: { $gt: 1 } },
      { sku: 1, parentSku: 1, itemQty: 1 }
    ).lean();

    // Map SKU -> product
    const productMap = new Map(products.map(p => [p.sku, p]));

    // Reprocess orders and generate rows
    for (const order of orders) {
      for (const item of order.items || []) {
        if (item.isCombo && item.comboSku && item.sku) {
          const product = productMap.get(item.sku);
          if (product) {
            resultRows.push({
              orderId: order.order_id,
              comboSku: item.comboSku,
              sku: item.sku,
              parentSku: product.parentSku || '',
              itemQty: product.itemQty,
            });
          }
        }
      }
    }

    // Write to CSV
    await csvWriter.writeRecords(resultRows);
    console.log('✅ CSV generated: combo_items_with_itemQty_gt_1.csv');

  } catch (err) {
    console.error('❌ Error generating CSV:', err);
  } finally {
    mongoose.disconnect();
  }
}

exportOrdersWithChildProductsQtyGT1();
