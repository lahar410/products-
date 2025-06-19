// const mongoose = require('mongoose');
// const fs = require('fs');
// const csv = require('csv-parser');

// // MongoDB Connection
// const MONGO_URI = 'mongodb://developer:pmq21byndxhte3wul7i5fjav9r4o06kg@beta.mongoserver.ko-tech.in:27017/?authSource=admin&directConnection=true&tls=true';

// mongoose.connect(MONGO_URI, {
//   useNewUrlParser: true,
//   useUnifiedTopology: true,
//   dbName: 'CRM-Database',
// }).then(() => console.log('MongoDB Connected')).catch(err => {
//   console.error('MongoDB Connection Error:', err);
//   process.exit(1);
// });

// // === Product Schema ===
// const productSchema = new mongoose.Schema({}, { strict: false });
// const Product = mongoose.model('Product', productSchema, 'products');

// // === Utility Function to Normalize SKU ===
// function normalizeSku(sku) {
//   if (!sku || typeof sku !== 'string') return '';
//   return sku.toUpperCase().replace(/\s+/g, '');
// }

// // === Process CSV and Update DB ===
// const updates = [];

// fs.createReadStream('parentSku.csv')
//   .pipe(csv())
//   .on('data', (row) => {
//     const rawSku = row['sku'];
//     const rawCorrectedParentSku = row['correctedParentSku'];

//     const sku = normalizeSku(rawSku);
//     const correctedParentSku = normalizeSku(rawCorrectedParentSku);
//     console.log(`parentSku == ${correctedParentSku}  sku == ${sku}`);
//     if (sku && correctedParentSku) {
//       const updatePromise = Product.findOneAndUpdate(
//         { sku: sku },
//         // { $set: { parentSku: correctedParentSku } },
//       ).then((doc) => {
//         if (doc) {
//           console.log(`Updated SKU: ${sku} -> ParentSKU: ${correctedParentSku}`);
//         } else {
//           console.warn(`SKU not found: ${sku}`);
//         }
//       }).catch(err => {
//         console.error(`Error updating SKU ${sku}:`, err.message);
//       });

//       updates.push(updatePromise);
//     }
//   })
//   .on('end', async () => {
//     console.log('CSV file processed. Waiting for DB updates...');
//     await Promise.all(updates);
//     console.log('All updates completed.');
//     mongoose.disconnect();
//   });




const mongoose = require('mongoose');
const fs = require('fs');
const csv = require('csv-parser');
const { format } = require('@fast-csv/format');

const MONGO_URI = 'mongodb://developer:pmq21byndxhte3wul7i5fjav9r4o06kg@beta.mongoserver.ko-tech.in:27017/?authSource=admin&directConnection=true&tls=true';

mongoose.connect(MONGO_URI, {
  useNewUrlParser: true,
  useUnifiedTopology: true,
  dbName: 'CRM-Database',
}).then(() => console.log('MongoDB Connected')).catch(err => {
  console.error('MongoDB Connection Error:', err);
  process.exit(1);
});

const productSchema = new mongoose.Schema({}, { strict: false });
const Product = mongoose.model('Product', productSchema, 'products');

function normalizeSku(sku) {
  if (!sku || typeof sku !== 'string') return '';
  return sku.toUpperCase().replace(/\s+/g, '');
}

// === Prepare CSV Writer for Results ===
const outputStream = fs.createWriteStream('updateResults.csv');
const csvStream = format({ headers: true });
csvStream.pipe(outputStream);

// === Process CSV and Update DB ===
const updates = [];

fs.createReadStream('parentSku.csv')
  .pipe(csv())
  .on('data', (row) => {
    const rawSku = row['sku'];
    const rawCorrectedParentSku = row['correctedParentSku'];

    const sku = normalizeSku(rawSku);
    const correctedParentSku = normalizeSku(rawCorrectedParentSku);

    if (sku && correctedParentSku) {
      const updatePromise = Product.findOneAndUpdate(
        { sku: sku },
        { $set: { parentSku: correctedParentSku } },
        { new: true }
      ).then((doc) => {
        if (doc) {
          console.log(`✅ Updated SKU: ${sku} -> ParentSKU: ${correctedParentSku}`);
          csvStream.write({ sku, correctedParentSku, status: 'updated' });
        } else {
          console.warn(`⚠️  SKU not found: ${sku}`);
          csvStream.write({ sku, correctedParentSku, status: 'not found' });
        }
      }).catch(err => {
        console.error(`❌ Error updating SKU ${sku}:`, err.message);
        csvStream.write({ sku, correctedParentSku, status: 'error', errorMessage: err.message });
      });

      updates.push(updatePromise);
    }
  })
  .on('end', async () => {
    console.log('CSV file processed. Waiting for DB updates...');
    await Promise.all(updates);
    csvStream.end();
    console.log('✅ All updates completed. Output written to updateResults.csv');
    mongoose.disconnect();
  });
