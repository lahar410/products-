require('dotenv').config();
const mongoose = require('mongoose');
const fs = require('fs');
const path = require('path');

// MongoDB connection
mongoose.connect(process.env.MONGODB_URI, {
     dbName: "CRM-Database",
  useNewUrlParser: true,
  useUnifiedTopology: true,
});

const db = mongoose.connection;
db.on('error', console.error.bind(console, 'MongoDB connection error:'));

// Define the Order schema (adjust as per your actual schema)
const orderSchema = new mongoose.Schema({
  order_id: String,
  total_weight: Number,
}, { collection: 'orders' });

const Order = mongoose.model('Order', orderSchema);

// Load JSON file
const weightsData = JSON.parse(fs.readFileSync(path.join(__dirname, 'positive_diff_orders.json'), 'utf8'));

async function updateWeights() {
  console.time("⏱ Update Time");
  let skipped = 0;

  const bulkOps = weightsData
    .filter(entry => entry.order_id && typeof entry.realWeight === 'number')
    .map(entry => ({
      updateOne: {
        filter: { order_id: entry.order_id },
        update: { $set: { total_weight: entry.realWeight } },
      }
    }));

  skipped = weightsData.length - bulkOps.length;

  if (bulkOps.length === 0) {
    console.warn("⚠️ No valid entries to process.");
    mongoose.disconnect();
    return;
  }

  try {
    const result = await Order.bulkWrite(bulkOps, { ordered: false });

    console.log(`✅ Total Matched: ${result.matchedCount}`);
    console.log(`✅ Total Modified: ${result.modifiedCount}`);
    console.log(`❌ Skipped Invalid Entries: ${skipped}`);
    console.timeEnd("⏱ Update Time");
  } catch (error) {
    console.error("❌ Error during bulk update:", error);
  } finally {
    mongoose.disconnect();
  }
}


updateWeights();

