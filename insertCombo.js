const mongoose = require('mongoose');
const fs = require('fs');
const csv = require('csv-parser');
const { format } = require('@fast-csv/format');
const path = require('path');

// MongoDB connection
mongoose.connect(
  'mongodb://developer:pmq21byndxhte3wul7i5fjav9r4o06kg@beta.mongoserver.ko-tech.in:27017/?authSource=admin&directConnection=true&tls=true',
  {
    // useNewUrlParser: true,
    // useUnifiedTopology: true,
    dbName: 'CRM-Database',
  }
).then(() => console.log('✅ MongoDB connected'))
  .catch(err => {
    console.error('❌ MongoDB connection error:', err);
    process.exit(1);
  });

const productSchema = new mongoose.Schema({
  sku: String,
  grossWeight: Number,
});

const comboSchema = new mongoose.Schema({
  sku: String,
  products: [
    {
      product: mongoose.Schema.Types.ObjectId,
      sku: String,
      itemQty: Number,
    },
  ],
  comboWeight: Number,
});

const Product = mongoose.model('Product', productSchema);
const Combo = mongoose.model('Combo', comboSchema);

const inputCsvPath = path.join(__dirname, 'combo_products_output33.csv');
const outputCsvPath = path.join(__dirname, 'updatedCombos33.csv');

const comboMap = new Map(); // { comboSku => [ { SKU, ParentSKU, ItemQty } ] }

fs.createReadStream(inputCsvPath)
  .pipe(csv())
  .on('data', (row) => {
    const comboSku = row['comboSku']?.trim();
    const SKU = row['SKU']?.trim();
    const ParentSKU = row['ParentSKU']?.trim();
    const ItemQty = parseInt(row['ItemQty']?.trim() || '1', 10);

    if (!comboMap.has(comboSku)) comboMap.set(comboSku, []);
    comboMap.get(comboSku).push({ SKU, ParentSKU, ItemQty });
  })
  .on('end', async () => {
    console.log('CSV file successfully processed.');

    const csvStream = format({ headers: true });
    const writableStream = fs.createWriteStream(outputCsvPath);
    csvStream.pipe(writableStream);

    for (const [comboSku, productsList] of comboMap.entries()) {
      try {
        const comboDoc = await Combo.findOne({ comboSku: comboSku });
        if (!comboDoc) {
          csvStream.write({ comboSku, status: 'Combo Not Found' });
          continue;
        }

        const updatedProducts = [];
        let totalGrossWeight = 0;

        for (const productInfo of productsList) {
          const productDoc = await Product.findOne({ sku: productInfo.ParentSKU });
          if (!productDoc) {
            csvStream.write({
              comboSku,
              status: `Product Not Found for SKU: ${productInfo.ParentSKU}`,
            });
            continue;
          }

          const itemQty = productInfo.ItemQty || 1;
          updatedProducts.push({
            product: productDoc._id,
            sku: productInfo.ParentSKU,
            itemQty,
          });

          totalGrossWeight += (productDoc.grossWeight ) * itemQty;
        }

        console.log("products are ,",updatedProducts,"weight ", totalGrossWeight)

        // Update combo
        comboDoc.products = updatedProducts;
        comboDoc.comboWeight = totalGrossWeight;
        await comboDoc.save();

        csvStream.write({ comboSku, status: 'Updated Successfully' });
      } catch (err) {
        console.error(`Error updating combo ${comboSku}:`, err);
        csvStream.write({ comboSku, status: 'Error - ' + err.message });
      }
    }

    csvStream.end(() => {
      console.log(`Update complete. Output written to ${outputCsvPath}`);
      mongoose.disconnect();
    });
  });
