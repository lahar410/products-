const mongoose = require('mongoose');
const fs = require('fs');
const csv = require('csv-parser');
const { format } = require('@fast-csv/format');

// ===== MongoDB Connection =====
const MONGO_URI =   'mongodb://developer:pmq21byndxhte3wul7i5fjav9r4o06kg@beta.mongoserver.ko-tech.in:27017/?authSource=admin&directConnection=true&tls=true'

mongoose.connect(MONGO_URI, {
  useNewUrlParser: true,
  useUnifiedTopology: true,
  dbName: 'CRM-Database', // <-- Change to your DB name
});
const db = mongoose.connection;
db.on('error', console.error.bind(console, 'MongoDB connection error:'));

// ====== Schemas ======
const productSchema = new mongoose.Schema({}, { strict: false });
const comboSchema = new mongoose.Schema({
  comboSku: { type: String, unique: true, required: true },
  products: [
    {
      product: { type: mongoose.Schema.Types.ObjectId, ref: 'Product', required: true },
      sku: { type: String, required: true },
    },
  ],
  name: { type: String },

  comboWeight: { type: Number },
});

const Product = mongoose.model('Product', productSchema, 'products'); // collection: products
const Combo = mongoose.model('Combo', comboSchema, 'combos'); // collection: combos

// ====== Read CSV and Process ======
const results = [];
fs.createReadStream('comboWeight-corrected.csv') // CSV should have headers: comboSku,name,comboWeight,products
  .pipe(csv({ headers: ['comboSku', 'name', 'comboWeight', 'products'] }))
  .on('data', (row) => results.push(row))
  .on('end', async () => {
    const report = [];

    for (const row of results) {
      const { comboSku, name, comboWeight, products } = row;
      const skuList = products.replace(/"/g, '').split(',').map(s => s.trim());
      const productObjects = [];

      for (const sku of skuList) {
        const prod = await Product.findOne({ sku });
        if (!prod) {
          report.push({ comboSku, status: 'Failed', reason: `SKU not found: ${sku}` });
          continue;
        }
        productObjects.push({ sku, product: prod._id });
      }

      if (productObjects.length !== skuList.length) {
        // Skip update if any SKU was missing
        continue;
      }

      const comboDoc = await Combo.findOne({ comboSku });
      if (!comboDoc) {
        report.push({ comboSku, status: 'Failed', reason: 'Combo not found' });
        continue;
      }

      // Update fields
      comboDoc.name = name;
      comboDoc.comboWeight = parseFloat(comboWeight);
      comboDoc.products = productObjects;

      try {
        await comboDoc.save();
        report.push({ comboSku, status: 'Success', reason: '' });
      } catch (err) {
        report.push({ comboSku, status: 'Failed', reason: err.message });
      }
    }

    // === Write Output CSV Report ===
    const output = fs.createWriteStream('combo_update_report22.csv');
    const csvStream = format({ headers: true });
    csvStream.pipe(output);
    report.forEach(r => csvStream.write(r));
    csvStream.end();

    console.log('Combo update complete. Report saved to combo_update_report.csv');
    mongoose.connection.close();
  });
