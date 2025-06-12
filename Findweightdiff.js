const { MongoClient } = require('mongodb');
const fs = require('fs');
const pLimit = require('p-limit').default;

const MONGODB_URI = 'mongodb://developer:pmq21byndxhte3wul7i5fjav9r4o06kg@beta.mongoserver.ko-tech.in:27017/?authSource=admin&directConnection=true&tls=true';
const DB_NAME = 'CRM-Database';

const ORDERS_COLLECTION = 'orders';
const PRODUCTS_COLLECTION = 'products';

const BATCH_SIZE = 5000;
const CONCURRENCY = 10;
const OUT_MISMATCH = 'totalWeight-0.5.json';
const OUT_MISSING = 'missing_product3.json';

async function checkOrderWeights() {
  const client = new MongoClient(MONGODB_URI);

  try {
    await client.connect();
    const db = client.db(DB_NAME);
    const ordersCol = db.collection(ORDERS_COLLECTION);
    const productsCol = db.collection(PRODUCTS_COLLECTION);

    console.time('Total-time');

    const matchStage = { items: { $exists: true, $not: { $size: 0 } } };
    const totalOrders = await ordersCol.countDocuments(matchStage);

    const mismatchedOrders = [];
    const missingSkuLogs = [];

    const limit = pLimit(CONCURRENCY);

    for (let skip = 0; skip < totalOrders; skip += BATCH_SIZE) {
      const orders = await ordersCol
        .find(matchStage)
        .skip(skip)
        .limit(BATCH_SIZE)
        .toArray();

      const allSKUs = new Set();
      for (const o of orders) {
        for (const it of o.items || []) {
          if (it.sku) allSKUs.add(it.sku.trim());
        }
      }

      const productDocs = await productsCol
        .find({ sku: { $in: [...allSKUs] } }, { projection: { sku: 1, grossWeight: 1 } })
        .toArray();

      const skuWeightMap = new Map(
        productDocs.map(d => [d.sku.trim(), d.grossWeight])
      );

      const tasks = orders.map(order =>
        limit(async () => {
          let realWeight = 0;
          let hasMissingSku = false;

          for (const { sku, qty } of order.items) {
            if (!sku || typeof qty !== 'number') continue;

            const trimmedSku = sku.trim().toUpperCase();
            const grossWeight = skuWeightMap.get(trimmedSku);

            if (typeof grossWeight === 'number') {
              realWeight += grossWeight * qty;
            } else {
              hasMissingSku = true;
              missingSkuLogs.push({
                orderId: order.order_id || order._id,
                sku: trimmedSku,
                reason: grossWeight === undefined
                  ? 'SKU not found in products'
                  : 'grossWeight missing / non-numeric'
              });
            }
          }

          if (hasMissingSku) return; // Skip this order for mismatch check

          const totalWeight = order.total_weight ?? 0;
          const diff = +(realWeight - totalWeight).toFixed(5);

          if (Math.abs(diff) >= 0.5) {
            mismatchedOrders.push({
              orderId: order.order_id || order._id,
              realWeight: +realWeight.toFixed(5),
              totalWeight: +totalWeight.toFixed(5),
              difference: diff
            });
          }
        })
      );

      await Promise.all(tasks);
      console.log(`✔︎ Batch ${skip} – ${Math.min(skip + BATCH_SIZE, totalOrders)} processed`);
    }

    fs.writeFileSync(OUT_MISMATCH, JSON.stringify(mismatchedOrders, null, 2));
    fs.writeFileSync(OUT_MISSING, JSON.stringify(missingSkuLogs, null, 2));

    console.log(`\nDone
       → Weight mismatches : ${mismatchedOrders.length}  (see ${OUT_MISMATCH})
       → Missing SKU items : ${missingSkuLogs.length}    (see ${OUT_MISSING})`);
    console.timeEnd('Total-time');

  } catch (err) {
    console.error('Error:', err);
  } finally {
    await client.close();
  }
}

checkOrderWeights();
